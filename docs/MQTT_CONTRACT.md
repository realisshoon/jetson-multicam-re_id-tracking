# MQTT Protocol & Topic Contract

This document defines the complete MQTT message specification, topic taxonomy, QoS levels, and message payload schemas across the CCTV Multi-Camera Tracking System.

---

## 1. Topic Taxonomy & Routing Matrix

| Topic | Publisher | Subscriber(s) | QoS | Description |
|---|---|---|---|---|
| `cctv/events/a/entry` | Camera A | Main Server | `1` | Person enters entry zone; contains initial Re-ID & Face embeddings |
| `cctv/events/a/timing` | Camera A | Main Server | `0` | Camera A periodic frame timing and node heartbeat |
| `cctv/responses/a/entry` | Main Server | Camera A | `1` | Server response assigning `journey_id`, `person_uid`, `global_id` |
| `cctv/candidates/b` | Main Server | Camera B | `1` | Server dispatches active candidates for passage matching |
| `cctv/events/b/passage` | Camera B | Main Server | `1` | Passage match event; contains B-node gallery and match scores |
| `cctv/candidates/c` | Main Server | Camera C | `1` | Server dispatches active candidates for passage matching |
| `cctv/events/c/passage` | Camera C | Main Server | `1` | Passage match event; contains C-node gallery and match scores |
| `cctv/candidates/d` | Main Server | Camera D | `1` | Server dispatches candidates with aggregated A+B/C galleries |
| `cctv/control/d/journey` | Main Server | Camera D | `1` | Journey lifecycle controls (`CANCELLED`, `FORCE_COMPLETE`, `EXPIRED`) |
| `cctv/events/d/arrival` | Camera D | Main Server | `1` | Destination arrival event with confirmed boundary crossing |
| `cctv/events/d/timing` | Camera D | Main Server | `0` | Camera D periodic frame timing and node heartbeat |
| `cctv/events/d/detection`| Camera D | Main Server / Web | `1` | Stranger detection event for persistent unregistered tracks |
| `cctv/main/journey/completed` | Main Server | Web Dashboard | `1` | Terminal broadcast of completed multi-camera journey |

---

## 2. Topic Schemas & Payloads

### `cctv/events/a/entry`
Published by Camera A upon detecting a person crossing the entry boundary:
```json
{
  "event_type": "ENTRY",
  "node_id": "A",
  "local_track_id": 12,
  "timestamp": "2026-08-18T11:00:00.000Z",
  "reid_features": [0.034, -0.012, "... 512 floats ..."],
  "face_features": [0.011, 0.045, "... 128 floats (optional) ..."],
  "body_candidates": [
    { "frame_index": 104, "quality": 0.85, "embedding": ["... 512 floats ..."] }
  ],
  "capture_path": "outputs/captures/A/track_12_entry.jpg"
}
```

### `cctv/responses/a/entry`
Sent from Main Server back to Camera A:
```json
{
  "node_id": "A",
  "local_track_id": 12,
  "journey_id": "J-20260818-0001",
  "person_uid": "P-00042",
  "global_id": 42,
  "status": "REGISTERED",
  "created_at": "2026-08-18T11:00:00.150Z"
}
```

### `cctv/candidates/b` and `cctv/candidates/c`
Dispatched by Main Server to intermediate nodes:
```json
{
  "journey_id": "J-20260818-0001",
  "person_uid": "P-00042",
  "global_id": 42,
  "stage": "WAITING_PASSAGE",
  "created_at": "2026-08-18T11:00:00.000Z",
  "gallery_samples": [
    { "source_node": "A", "quality": 0.85, "embedding": ["... 512 floats ..."] }
  ]
}
```

### `cctv/events/b/passage` and `cctv/events/c/passage`
Published by Camera B/C upon passage matching:
```json
{
  "event_type": "PASSAGE",
  "node_id": "B",
  "journey_id": "J-20260818-0001",
  "person_uid": "P-00042",
  "local_track_id": 8,
  "best_score": 0.82,
  "topk_score": 0.78,
  "combined_score": 0.798,
  "passed_at": "2026-08-18T11:00:15.200Z",
  "gallery_samples": [
    { "source_node": "B", "quality": 0.88, "embedding": ["... 512 floats ..."] }
  ]
}
```

### `cctv/candidates/d`
Dispatched by Main Server to destination Node D:
```json
{
  "journey_id": "J-20260818-0001",
  "person_uid": "P-00042",
  "global_id": 42,
  "stage": "WAITING_D",
  "route": ["A", "B"],
  "gallery_samples": [
    { "source_node": "A", "embedding": ["... 512 floats ..."] },
    { "source_node": "B", "embedding": ["... 512 floats ..."] }
  ]
}
```

### `cctv/events/d/arrival`
Published by Camera D when destination arrival is verified:
```json
{
  "event_type": "ARRIVAL",
  "node_id": "D",
  "journey_id": "J-20260818-0001",
  "person_uid": "P-00042",
  "local_track_id": 5,
  "arrival_score": 0.84,
  "confirmed": true,
  "arrived_at": "2026-08-18T11:00:32.400Z"
}
```

### `cctv/events/d/detection`
Published by Camera D for stranger / unregistered tracks:
```json
{
  "detection_id": "D-20260818T110035-L5",
  "at": "2026-08-18T11:00:35Z",
  "node": "D",
  "kind": "STRANGER_DETECTED",
  "identity_status": "UNREGISTERED",
  "local_track_id": 5,
  "journey_id": null,
  "person_uid": null,
  "canonical_person_uid": null
}
```
