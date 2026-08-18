"""
jetson-multicam-re_id-tracking 이 MQTT(cctv/entry 토픽)로 발행하는 ENTRY
이벤트를 DB 레코드로 적재한다.

jetson 리포는 절대 수정하지 않는다. 이 모듈은 그쪽 src/nodes/node_a.py 가
이미 브로커로 보내는 메시지를 읽기만 하는 '제3의 구독자' 입장이다
(src/nodes/node_b.py 가 같은 토픽을 구독하는 것과 동일한 방식).

기대하는 페이로드 스키마 (node_a.py 의 mqtt_publisher.publish_entry 호출부 기준):
    {
      "timestamp": "2026-08-06T10:00:00",
      "node_id": "A",
      "event": "ENTRY",
      "local_track_id": 3,
      "global_person_id": "G000001",
      "next_nodes": ["B", "C"],
      "reid_model": "osnet_x0_25",
      "embedding_dim": 512,
      "embedding": [0.01, -0.02, ...]   # 512-d
    }
"""
from __future__ import annotations

from typing import Any

import numpy as np
from django.utils import timezone

from .models import Camera, Event, Person, Snapshot, Tracklet

# Camera.index 는 unique 필드다. 로컬에서 직접 관리하는 카메라(0~99)와
# 겹치지 않도록 jetson 노드 전용으로 900번대를 예약해 둔다.
NODE_CAMERAS = {
    "A": {"index": 900, "name": "Camera A · 입장"},
    "B": {"index": 901, "name": "Camera B · 재식별"},
    "C": {"index": 902, "name": "Camera C · 미가동"},
    "D": {"index": 903, "name": "Camera D · 도착"},
}


def _get_camera(node_id: str) -> Camera:
    info = NODE_CAMERAS.get(node_id, {"index": 999, "name": f"Camera {node_id}"})
    cam, _ = Camera.objects.get_or_create(
        index=info["index"],
        defaults={"name": info["name"], "source": f"jetson:{node_id}",
                  "note": "jetson-multicam-re_id-tracking 자동 등록"},
    )
    return cam


def ingest_entry_payload(payload: dict[str, Any]) -> Person | None:
    """ENTRY 페이로드 하나를 저장한다. ENTRY 가 아니거나 global_person_id 가
    없으면 아무 것도 하지 않고 None 을 반환한다."""
    if payload.get("event") != "ENTRY":
        return None

    global_id = payload.get("global_person_id")
    if not global_id:
        return None

    node_id = payload.get("node_id") or "A"
    local_track_id = payload.get("local_track_id")
    embedding = payload.get("embedding")

    now = timezone.now()
    cam = _get_camera(node_id)

    person, created = Person.objects.get_or_create(
        external_id=global_id,
        defaults={"created_at": now, "last_seen": now},
    )
    if not created:
        person.last_seen = now
        person.save(update_fields=["last_seen"])

    # MQTT QoS 1 은 "최소 1회" 배달이라 같은 메시지가 재전송될 수 있다.
    # node_a.py 는 local_id 하나당 ENTRY 를 정확히 한 번만 발행하므로,
    # 이 (camera, local_id) 트랙렛이 이미 있으면 재전송으로 보고 건너뛴다.
    tracklet_is_new = True
    if local_track_id is not None:
        _tracklet, tracklet_is_new = Tracklet.objects.get_or_create(
            person=person, camera=cam, local_id=int(local_track_id),
            defaults={"start_at": now, "end_at": now, "frames": 1},
        )

    if not tracklet_is_new:
        return person

    Event.objects.create(person=person, camera=cam, kind=Event.ENTER, at=now,
                         detail=f"jetson {global_id} (local #{local_track_id})")

    if isinstance(embedding, list) and embedding:
        vec = np.asarray(embedding, dtype=np.float32)
        if vec.size:
            snap = Snapshot(person=person, score=1.0, created_at=now)
            snap.set_vector(vec)
            snap.save()

    return person
