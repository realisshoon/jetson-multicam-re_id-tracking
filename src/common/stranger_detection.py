from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import paho.mqtt.client as mqtt


STRANGER_DETECTION_TOPICS = {
    "B": "cctv/events/b/detection",
    "C": "cctv/events/c/detection",
    "D": "cctv/events/d/detection",
}
STRANGER_STABLE_SECONDS = 2.0
STRANGER_VALID_FRAMES = 10
STRANGER_PUBACK_TIMEOUT_SECONDS = 5.0


@dataclass
class StrangerTrackState:
    first_unregistered_at: datetime | None = None
    valid_frame_count: int = 0
    identity_confirmed: bool = False
    emitted: bool = False


@dataclass(frozen=True)
class StrangerPublishResult:
    detection_id: str
    rc: int | None
    mid: int | None
    puback_received: bool
    failed: bool
    timeout: bool
    error: str | None


class StrangerDetectionGate:
    """Track-scoped stability gate; it never changes Re-ID decisions."""

    def __init__(
        self,
        node: str,
        stable_seconds: float = STRANGER_STABLE_SECONDS,
        valid_frames: int = STRANGER_VALID_FRAMES,
    ) -> None:
        normalized_node = node.upper()
        if normalized_node not in STRANGER_DETECTION_TOPICS:
            raise ValueError(f"Unsupported stranger detection node: {node}")
        if stable_seconds <= 0:
            raise ValueError("stable_seconds must be positive")
        if valid_frames <= 0:
            raise ValueError("valid_frames must be positive")
        self.node = normalized_node
        self.stable_seconds = stable_seconds
        self.valid_frames = valid_frames
        self._tracks: dict[int, StrangerTrackState] = {}
        self._issued_detection_ids: set[str] = set()

    @property
    def topic(self) -> str:
        return STRANGER_DETECTION_TOPICS[self.node]

    def observe(
        self,
        *,
        local_track_id: int,
        observed_at: datetime,
        is_unregistered: bool,
        matching_in_progress: bool,
    ) -> dict[str, Any] | None:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")

        state = self._tracks.setdefault(local_track_id, StrangerTrackState())
        if state.emitted or state.identity_confirmed:
            return None

        if not is_unregistered:
            state.identity_confirmed = True
            state.first_unregistered_at = None
            state.valid_frame_count = 0
            return None

        if matching_in_progress:
            state.first_unregistered_at = None
            state.valid_frame_count = 0
            return None

        if state.first_unregistered_at is None:
            state.first_unregistered_at = observed_at
        state.valid_frame_count += 1
        stable_for = (observed_at - state.first_unregistered_at).total_seconds()
        if (
            stable_for < self.stable_seconds
            and state.valid_frame_count < self.valid_frames
        ):
            return None

        state.emitted = True
        at = observed_at.isoformat(timespec="seconds")
        detection_id_base = (
            f"{self.node}-{observed_at.strftime('%Y%m%dT%H%M%S')}-"
            f"L{local_track_id}"
        )
        detection_id = detection_id_base
        retry_suffix = 2
        while detection_id in self._issued_detection_ids:
            detection_id = f"{detection_id_base}-R{retry_suffix}"
            retry_suffix += 1
        self._issued_detection_ids.add(detection_id)
        return {
            "detection_id": detection_id,
            "at": at,
            "node": self.node,
            "kind": "STRANGER_DETECTED",
            "identity_status": "UNREGISTERED",
            "local_track_id": local_track_id,
            "journey_id": None,
            "person_uid": None,
            "canonical_person_uid": None,
        }

    def remove_track(self, local_track_id: int) -> None:
        self._tracks.pop(local_track_id, None)


def publish_stranger_detection(
    client: mqtt.Client,
    topic: str,
    payload: dict[str, Any],
    *,
    qos: int,
    timeout_seconds: float = STRANGER_PUBACK_TIMEOUT_SECONDS,
) -> StrangerPublishResult:
    detection_id = str(payload["detection_id"])
    info = None
    rc: int | None = None
    mid: int | None = None
    puback_received = False
    failed = False
    timed_out = False
    error_text: str | None = None

    try:
        info = client.publish(
            topic,
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            qos=qos,
            retain=False,
        )
        rc = int(info.rc)
        mid = int(info.mid)
        if rc == int(mqtt.MQTT_ERR_SUCCESS):
            info.wait_for_publish(timeout=timeout_seconds)
            published = bool(info.is_published())
            puback_received = published if qos > 0 else False
            timed_out = not published
            failed = timed_out
        else:
            failed = True
            error_text = mqtt.error_string(rc)
    except (
        AttributeError,
        RuntimeError,
        TypeError,
        ValueError,
        OSError,
    ) as error:
        failed = True
        error_text = f"{type(error).__name__}: {error}"
        if info is not None:
            try:
                rc = int(info.rc)
            except (AttributeError, TypeError, ValueError):
                pass
        published = False
        if info is not None and hasattr(info, "is_published"):
            try:
                published = bool(info.is_published())
            except (RuntimeError, ValueError):
                published = False
        timed_out = rc == int(mqtt.MQTT_ERR_SUCCESS) and not published

    result = StrangerPublishResult(
        detection_id=detection_id,
        rc=rc,
        mid=mid,
        puback_received=puback_received,
        failed=failed,
        timeout=timed_out,
        error=error_text,
    )
    print(
        f"[{payload['node']} STRANGER MQTT] "
        f"detection_id={detection_id} topic={topic} rc={rc} mid={mid} "
        f"puback={puback_received} failed={failed} timeout={timed_out}"
        + (f" error={error_text}" if error_text else "")
    )
    return result
