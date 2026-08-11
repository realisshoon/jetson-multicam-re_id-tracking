#!/usr/bin/env python
"""
jetson-multicam-re_id-tracking 연동 프로세스. Django 와, 그리고 로컬 카메라
파이프라인(tracker_worker.py) 과도 **별도 프로세스**로 돈다.

이 레포는 jetson 쪽 리포를 절대 수정하지 않는다. jetson 은 별도 네트워크의
Jetson 장비에서 완전히 독립적으로 돈다(카메라 캡처도, YOLO/ByteTrack/Re-ID
추론도 이미 그쪽에서 끝낸다). 이 워커가 하는 일은 그쪽이 이미 브로커로
발행하는 MQTT cctv/entry 토픽을 구독해서, 들어오는 입장(ENTRY) 이벤트를
DB 에 적재하는 것뿐이다 — jetson 의 src/nodes/node_b.py 가 같은 토픽을
구독하는 것과 동일한 방식의 '제3의 구독자'.

영상은 이 워커를 거치지 않는다. node_a.py/node_b.py 가 :8000/:8001 에서
직접 서빙하는 MJPEG 스트림을 대시보드가 그대로 임베드한다.

접속 주소(IP/포트)는 더 이상 여기 코드나 settings.py 에 박혀있지 않다 —
RuntimeConfig(DB, 대시보드 설정 패널에서 편집)가 기준이라 이 워커가
1초마다 확인해서 바뀌면 알아서 재연결한다(아래 main() 참고).

Django ORM 은 django.setup() 만 부르면 그대로 쓸 수 있다.

실행:  python mqtt_worker.py
"""
import json
import os
import time

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()                                    # ← ORM 쓰려면 반드시 먼저

import paho.mqtt.client as mqtt                   # noqa: E402
from django.conf import settings                  # noqa: E402

from tracking import bus                          # noqa: E402
from tracking.models import RuntimeConfig          # noqa: E402
from tracking.mqtt_ingest import ingest_entry_payload  # noqa: E402

MQTT_TOPIC = settings.JETSON["MQTT_TOPIC"]
STATE_KEY = "state:mqtt"          # 로컬 카메라 파이프라인의 state:live 와 분리

worker_state = {"connected": False, "entries_total": 0}


def on_connect(client, userdata, flags, reason_code, properties):
    worker_state["connected"] = (reason_code == 0)
    if worker_state["connected"]:
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[mqtt] 연결 완료 / 구독: {MQTT_TOPIC}")
    else:
        print(f"[mqtt] 연결 실패: reason_code={reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties=None):
    worker_state["connected"] = False
    print(f"[mqtt] 연결 끊김: reason_code={reason_code}")


def on_message(client, userdata, message: mqtt.MQTTMessage):
    try:
        payload = json.loads(message.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"[mqtt] 잘못된 메시지: {error}")
        return

    person = ingest_entry_payload(payload)
    if person is None:
        return

    worker_state["entries_total"] += 1
    print(f"[mqtt] 입장 적재: {payload.get('global_person_id')} "
          f"(누적 {worker_state['entries_total']}명)")


def _build_client():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="reid_admin_web_ingest")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def main():
    # paho 클라이언트를 하나 붙잡고 connect_async()/disconnect() 만 번갈아
    # 부르는 방식은 내부 상태 전이가 애매해서(loop_start() 로 이미 돌고
    # 있는 중에 connect_async() 를 다시 불러도 재접속이 안 되는 경우가
    # 있었다) — 재연결할 때마다 클라이언트를 통째로 새로 만든다. 매번
    # "맨 처음 접속"과 완전히 같은 경로를 타니 훨씬 안정적이다.
    client = None
    connected_target = False   # 지금 "붙어 있어야 하는" 상태인지 (실제 연결 성공 여부와는 별개)

    def start(host, port):
        nonlocal client, connected_target
        client = _build_client()
        print(f"[mqtt] 접속 시도(비동기): {host}:{port}")
        # connect_async + loop_start: 브로커가 아직 안 떠 있어도 죽지 않고
        # 백그라운드에서 계속 재시도한다 (jetson 장비가 나중에 켜지는 경우 대비).
        client.connect_async(host, port, keepalive=60)
        client.loop_start()
        connected_target = True

    def stop():
        nonlocal connected_target
        client.loop_stop()
        client.disconnect()
        worker_state["connected"] = False
        connected_target = False

    cfg = RuntimeConfig.get()
    host, port = cfg.jetson_host, cfg.jetson_mqtt_port
    if cfg.detection_enabled:
        start(host, port)

    try:
        # 1초마다 (1) 연결 상태를 bus 에 올려서 대시보드가 폴링하게 하고,
        # (2) RuntimeConfig(대시보드 설정 패널에서 저장한 값)가 바뀌었는지
        # 확인한다. Jetson 장비 자체(카메라·YOLO·Re-ID)는 원격으로 켜고
        # 끌 방법이 없어서 항상 자체적으로 돈다 — "감지 on/off" 는 여기서
        # 브로커 연결을 끊고 잇는 걸로 흉내낸다. 꺼두면 새 감지 이벤트를
        # 아예 받지도 처리하지도 않는다(영상은 브라우저가 Jetson 에 직접
        # 붙는 거라 이 스위치와 무관하게 계속 나온다). IP/포트가 바뀌었을
        # 때도 마찬가지로 여기서 알아서 재연결한다 — 워커를 재시작할
        # 필요가 없다.
        while True:
            cfg = RuntimeConfig.get()
            host_changed = (cfg.jetson_host, cfg.jetson_mqtt_port) != (host, port)
            host, port = cfg.jetson_host, cfg.jetson_mqtt_port

            if cfg.detection_enabled and not connected_target:
                print("[mqtt] 감지 켜짐 → 재접속")
                start(host, port)
            elif not cfg.detection_enabled and connected_target:
                print("[mqtt] 감지 꺼짐 → 연결 끊음(새 이벤트 수신 중단)")
                stop()
            elif cfg.detection_enabled and host_changed:
                print(f"[mqtt] 설정 변경 감지 → 재접속: {host}:{port}")
                stop()
                start(host, port)

            bus.publish_state({
                "mqtt_connected": worker_state["connected"],
                "entries_total": worker_state["entries_total"],
            }, key=STATE_KEY)
            time.sleep(1.0)
    except KeyboardInterrupt:
        print()
        print("[mqtt] 종료")
    finally:
        if connected_target:
            stop()


if __name__ == "__main__":
    main()
