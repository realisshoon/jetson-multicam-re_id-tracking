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
직접 서빙하는 MJPEG 스트림을 대시보드가 그대로 임베드한다
(config/settings.py 의 JETSON["CAM_A_STREAM_URL"] 등 참고).

Django ORM 은 django.setup() 만 부르면 그대로 쓸 수 있다.

실행:  python mqtt_worker.py
환경변수: JETSON_MQTT_HOST, JETSON_MQTT_PORT, JETSON_MQTT_TOPIC
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
from tracking.mqtt_ingest import ingest_entry_payload  # noqa: E402

JETSON = settings.JETSON
STATE_KEY = "state:mqtt"          # 로컬 카메라 파이프라인의 state:live 와 분리

worker_state = {"connected": False, "entries_total": 0}


def on_connect(client, userdata, flags, reason_code, properties):
    worker_state["connected"] = (reason_code == 0)
    if worker_state["connected"]:
        client.subscribe(JETSON["MQTT_TOPIC"], qos=1)
        print(f"[mqtt] 연결 완료: {JETSON['MQTT_HOST']}:{JETSON['MQTT_PORT']}")
        print(f"[mqtt] 구독: {JETSON['MQTT_TOPIC']}")
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


def publish_state_loop():
    """1초마다 현재 MQTT 연결 상태를 bus 에 올린다. 대시보드가 이걸 폴링한다."""
    while True:
        bus.publish_state({
            "mqtt_connected": worker_state["connected"],
            "entries_total": worker_state["entries_total"],
        }, key=STATE_KEY)
        time.sleep(1.0)


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="reid_admin_web_ingest")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    print(f"[mqtt] 접속 시도(비동기): {JETSON['MQTT_HOST']}:{JETSON['MQTT_PORT']}")
    # connect_async + loop_start: 브로커가 아직 안 떠 있어도 죽지 않고
    # 백그라운드에서 계속 재시도한다 (jetson 장비가 나중에 켜지는 경우 대비).
    client.connect_async(JETSON["MQTT_HOST"], JETSON["MQTT_PORT"], keepalive=60)
    client.loop_start()

    try:
        publish_state_loop()
    except KeyboardInterrupt:
        print()
        print("[mqtt] 종료")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
