# Camera D Integration Contract

This document specifies the handoff contract and environment state for `src.nodes.node_d` at the Camera D final-freeze milestone. It accurately reflects current operating behavior and environment settings without altering thresholds or runtime logic.

---

## 1. Role and Production Flow

Camera D operates as the terminal destination node (Node ID `D`) in the multi-camera tracking pipeline:

```text
Central Broker (cctv/candidates/d)
  -> Candidate Queue & Storage (WAITING_D stage: Journey ID / Person UID / A+B/C Gallery)
  -> Journey Control (cctv/control/d/journey: CANCELLED / FORCE_COMPLETE)
  -> Camera Stream (/dev/video0, 640x480 @ 15 FPS V4L2 -> Display Stream 1280x720 JPEG @ Q80)
  -> YOLO Person Detection (yolo26n.pt, conf=0.50, iou=0.50)
  -> ByteTrack Local Multi-Object Tracking (bytetrack.yaml)
  -> Body Crop Extraction & Preprocessing (256x128 BGR Normalized)
  -> OSNet FP16 TensorRT 512-D Embedding Extraction
  -> Entry Boundary Crossing Gate (boundary_band_ratio=0.15, interior_band_ratio=0.20)
  -> Temporal Guardrails Check (clock tolerance 2.0s, passage duration 1.0s..300.0s)
  -> Multi-Candidate Similarity Matching (Best >= 0.70, Top2 >= 0.62, Combined margin >= 0.04)
  -> Multi-Pass Confirmation Window (Window=5, Required=4, Interval>=0.5s, Spread<=0.08)
  -> D ARRIVAL Event Validation & QoS 1 MQTT Publish (cctv/events/d/arrival)
  -> Periodic Node Timing Event Publish (cctv/events/d/timing)
  -> Stranger Detection Gate (2.0s stable, 10 frames) -> Publish (cctv/events/d/detection)
```

---

## 2. Start Command

From the repository root, using the repo-local virtual environment:

```bash
./scripts/run_node_d.sh
```

Or directly:

```bash
.venv/bin/python -m src.nodes.node_d
```

---

## 3. Runtime Endpoints and Identifiers

| Item | Audited Specification |
|---|---|
| Node ID | `D` |
| MQTT Client ID | `camera-d` |
| MQTT Host | `10.10.20.33` (via `configs/mqtt.yaml` / `JETSON_MQTT_CONFIG` / `configs/mqtt_config.yaml`) |
| MQTT Port | `1883` |
| MQTT QoS | `1` |
| Candidate Subscribe Topic | `cctv/candidates/d` |
| Journey Control Subscribe Topic | `cctv/control/d/journey` |
| Arrival Publish Topic | `cctv/events/d/arrival` |
| Timing Publish Topic | `cctv/events/d/timing` |
| Stranger Detection Publish Topic | `cctv/events/d/detection` (from `STRANGER_DETECTION_TOPICS["D"]`) |
| Web Server Bind | `0.0.0.0:8003` |
| Web Routes | `/`, `/stream`, `/captures/D/...` |
| Camera Device | `/dev/video0` (V4L2, 640x480 @ 15 FPS capture; stream output 1280x720 JPEG @ Q80) |
| Frame Transforms | Horizontal flip = True, Contrast alpha = 1.02, Brightness beta = 8 |

---

## 4. Models and Paths

| Model | Path | Purpose | Status |
|---|---|---|---|
| YOLO26n | `yolo26n.pt` | Person detection feeding ByteTrack | Verified |
| OSNet x0.25 FP16 Engine | `models/reid/person_reid_osnet_x0_25_fp16.engine` | 512-D TensorRT body Re-ID embedding | Verified |

---

## 5. Frozen Runtime Settings

### Core Thresholds and Parameters

| Parameter | Frozen Value | Source / Note |
|---|---:|---|
| `MATCH_BEST_THRESHOLD` | 0.70 | Minimum best similarity threshold |
| `MATCH_TOP2_THRESHOLD` | 0.62 | Minimum top-2 average similarity threshold |
| `MATCH_MARGIN` | 0.04 | `min_journey_margin` from `configs/node_d_matching.yaml` |
| `MATCH_CONFIRMATIONS` | 4 | `confirmation_required_passes` from `configs/node_d_matching.yaml` |
| `VERIFY_THRESHOLD` | 0.55 | Continuity verification threshold for matched tracks |
| `VERIFY_FAILURE_LIMIT` | 2 | Maximum verification failure count before unlinking |
| `REID_INTERVAL_FRAMES` | 3 | Frame interval for active track Re-ID feature extraction |
| `REID_HISTORY_SIZE` | 5 | Track embedding history queue capacity |
| `CANDIDATE_TIMEOUT_SECONDS` | 300.0 | Max wait time for candidate arrival (`max_passage_to_d_seconds`) |
| `ARRIVAL_PUBACK_TIMEOUT_SECONDS` | 5.0 | Timeout for QoS 1 PUBACK confirmation |
| `ANOMALY_DELAY_SECONDS` | 2.0 | Delay threshold before anomaly trigger |
| `TRACK_LOST_GRACE_FRAMES` | 20 | Grace period frame count for lost tracks |

### Matching Guardrails & Temporal Rejection (`configs/node_d_matching.yaml`)

| Setting | Value | Description |
|---|---:|---|
| `clock_tolerance_seconds` | 2.0s | Max allowable clock drift between nodes |
| `min_passage_to_d_seconds` | 1.0s | Minimum travel duration from B/C passage to D arrival (`TOO_EARLY`) |
| `max_passage_to_d_seconds` | 300.0s | Maximum valid travel duration (`EXPIRED_JOURNEY`) |
| `boundary_band_ratio` | 0.15 | Normalized outer border region for entry detection |
| `interior_band_ratio` | 0.20 | Normalized inner region required for crossing confirmation |
| `confirmation_window_size` | 5 | Number of consecutive evaluation samples in window |
| `confirmation_required_passes` | 4 | Minimum required passing samples within window |
| `confirmation_min_sample_interval_seconds` | 0.5s | Minimum time between consecutive confirmation samples |
| `confirmation_max_score_spread` | 0.08 | Maximum score deviation allowed before window reset |
| `min_journey_margin` | 0.04 | Margin required over runner-up candidate score |

### Stranger Detection Gate (`src/common/stranger_detection.py`)

| Parameter | Value | Description |
|---|---:|---|
| `STRANGER_STABLE_SECONDS` | 2.0s | Minimum continuous observation duration for unregistered track |
| `STRANGER_VALID_FRAMES` | 10 | Minimum frame count for unregistered track before event publish |
| `STRANGER_PUBACK_TIMEOUT_SECONDS` | 5.0s | Timeout for QoS 1 stranger event PUBACK confirmation |
| Topic | `cctv/events/d/detection` | Node D detection topic |

---

## 6. Environment and Dependency Comparison

### System Environment
- **Host / OS**: Linux ubuntu 5.15.185-tegra aarch64 (Jetson Orin)
- **JetPack / L4T**: JetPack 6.1 (`nvidia-l4t-core`), L4T R36.5.0
- **CUDA Toolkit**: 12.6 (`nvcc 12.6.68`)
- **cuDNN**: 9.3.0.75 (`libcudnn9-cuda-12`)
- **TensorRT**: 10.3.0.30 (`libnvinfer10`, Python binding `tensorrt==10.3.0`)
- **Python**: 3.10.12 (`.venv/bin/python`)

### Direct Dependency Comparison

| Package | Camera A | Camera B | Camera C | Camera D | Status |
|---|---|---|---|---|---|
| `numpy` | 1.26.4 | 1.26.4 | 1.26.4 | 1.26.4 | **Exact Match** |
| `paho-mqtt` | 2.1.0 | 2.1.0 | 2.1.0 | 2.1.0 | **Exact Match** |
| `PyYAML` | 6.0.2 | 6.0.2 | 6.0.2 | 6.0.2 | **Exact Match** |
| `ultralytics` | 8.4.112 | 8.4.112 | 8.4.112 | 8.4.112 | **Exact Match** |
| `lap` | 0.5.12 | 0.5.12 | 0.5.12 | 0.5.12 | **Exact Match** |
| `tensorrt` | 10.3.0 | 10.3.0 | 10.3.0 | 10.3.0 | **Exact Match** |
| `torch` | 2.8.0 | 2.3.0 | 2.3.0 | 2.3.0 | **Compatible** (JetPack 6 wheel) |
| `torchvision` | 0.23.0 | 0.18.0a0+6043bc2 | 0.18.0a0+6043bc2 | 0.18.0a0+6043bc2 | **Compatible** (JetPack 6 wheel) |
| `opencv` | 4.11.0.86 | 4.11.0.86 | 4.11.0.86 | 4.8.0 | **Compatible** |

---

## 7. Validation Commands

Safe, non-hardware validation:

```bash
.venv/bin/python -m py_compile \
  src/nodes/node_d.py \
  src/common/node_d_matching.py \
  src/common/stranger_detection.py \
  src/network/mqtt_client.py \
  src/common/config.py \
  src/reid/reid_engine.py \
  src/reid/preprocess.py

.venv/bin/python -c "import src.nodes.node_d; print('Camera D import: PASS')"
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -p "test_node_d*.py"
PYTHONPATH=. .venv/bin/python -m unittest tests/test_stranger_detection.py
```
