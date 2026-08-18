# Camera B Integration Contract

This document specifies the handoff contract and environment state for `src.nodes.node_b` at the Camera B final-freeze milestone. It accurately reflects current operating behavior and environment settings without altering thresholds or runtime logic.

---

## 1. Role and Production Flow

Camera B operates as intermediate Node ID `B` in the multi-camera tracking pipeline:

```text
Central Broker (cctv/candidates/b)
  -> Candidate Queue & Storage (Journey ID / Person UID / A Gallery)
  -> Camera Stream (/dev/video0, 1280x720 @ 30 FPS, V4L2)
  -> YOLO Person Detection (yolo26n.pt, conf=0.50, iou=0.50)
  -> ByteTrack Local Multi-Object Tracking (bytetrack.yaml)
  -> Body Crop Extraction & Preprocessing (256x128 BGR Normalized)
  -> OSNet FP16 TensorRT 512-D Embedding Extraction
  -> Multi-frame Candidate Similarity Matching (Cosine similarity vs A Gallery)
  -> Temporal Window Aggregation & High-Quality Gallery Selection (Target: 2)
  -> B PASSAGE Event Validation & MQTT Publish (cctv/events/b/passage)
```

---

## 2. Start Command

From the repository root, using the repo-local virtual environment:

```bash
./scripts/run_node_b.sh
```

Or directly:

```bash
.venv/bin/python -m src.nodes.node_b
```

---

## 3. Runtime Endpoints and Identifiers

| Item | Audited Specification |
|---|---|
| Node ID | `B` |
| MQTT Client ID | `camera-b` |
| MQTT Host | `10.10.20.33` (via `configs/mqtt.yaml` / `JETSON_MQTT_CONFIG`) |
| MQTT Port | `1883` |
| MQTT QoS | `1` |
| Candidate Subscribe Topic | `cctv/candidates/b` |
| Passage Publish Topic | `cctv/events/b/passage` |
| Web Server Bind | `0.0.0.0:8001` |
| Web Routes | `/`, `/stream`, `/captures/B/...`, `/metrics`, `/health`, `/status` |
| Camera Device | `/dev/video0` (V4L2, 1280x720, requested 30 FPS, MJPEG) |
| Frame Transforms | Horizontal flip = True, Contrast alpha = 1.02, Brightness beta = 8 |

---

## 4. Models and Paths

| Model | Path | Purpose | Size / Status |
|---|---|---|---|
| YOLO26n | `yolo26n.pt` | Person detection feeding ByteTrack | 5.3 MB (Verified) |
| OSNet x0.25 FP16 Engine | `models/reid/person_reid_osnet_x0_25_fp16.engine` | 512-D TensorRT body Re-ID embedding | 1.6 MB (Verified) |

*Note: Camera B does not perform face recognition; YuNet and SFace models are not required for B runtime.*

---

## 5. Frozen Runtime Settings

| Parameter | Frozen Value | Note |
|---|---:|---|
| YOLO Confidence / IoU | 0.50 / 0.50 | Person class (`classes=[0]`) |
| Tracker Backend | `bytetrack.yaml` | `lap==0.5.12` |
| Match Threshold | 0.70 | Minimum similarity to begin match verification |
| Match Margin | 0.05 | Margin over runner-up candidate |
| Match Confirmations | 3 | Consecutive match count required |
| Verify Threshold | 0.55 | Tracking continuity threshold |
| Verify Failure Limit | 2 | Max consecutive verification misses |
| Re-ID Interval Frames | 3 | Inference interval for active tracks |
| B Gallery Target | 2 | Required number of B-node gallery embeddings |
| Min Quality Threshold | 0.70 | Minimum crop quality for passage inclusion |
| Passage Min Best Score | 0.75 | Gate for valid passage event |
| Passage Min Top-K Score | 0.68 | Gate for valid passage event |
| Passage Min Combined Score | 0.72 | Weighted combination score |
| Candidate Timeout | 300.0s | Max wait time for candidate arrival |

---

## 6. Environment and Dependency Comparison with Camera A

### System Environment
- **Host / OS**: Linux ubuntu 5.15.185-tegra aarch64 (Jetson Orin)
- **JetPack / L4T**: JetPack 6.2.3+b81 (`nvidia-jetpack`), L4T R36.5.0
- **CUDA Toolkit**: 12.6 (`nvcc 12.6.68`)
- **cuDNN**: 9.3.0.75 (`libcudnn9-cuda-12`)
- **TensorRT**: 10.3.0.30 (`libnvinfer10`, Python binding `tensorrt==10.3.0`)
- **Python**: 3.10.12 (`.venv/bin/python`)

### Direct Dependency Delta Table

| Package | Camera A (`requirements/jetson.txt`) | Camera B (`camera-b.freeze.txt`) | Status / Delta Classification |
|---|---|---|---|
| `numpy` | 1.26.4 | 1.26.4 | **Exact Match** |
| `opencv-python` | 4.11.0.86 | 4.11.0.86 | **Exact Match** |
| `paho-mqtt` | 2.1.0 | 2.1.0 | **Exact Match** |
| `PyYAML` | 6.0.2 | 6.0.2 | **Exact Match** |
| `ultralytics` | 8.4.112 | 8.4.112 | **Exact Match** |
| `lap` | 0.5.12 | 0.5.12 | **Exact Match** |
| `tensorrt` | 10.3.0 | 10.3.0 | **Exact Match** (JetPack system binding) |
| `torch` | 2.8.0 | 2.3.0 | **Version Difference**: Camera A had custom torch 2.8.0 build; Camera B uses official JetPack 6 wheel `torch-2.3.0-cp310-cp310-linux_aarch64.whl` (CUDA 12.4 runtime backend). Both are functionally compatible for Re-ID TensorRT bridge. |
| `torchvision` | 0.23.0 | 0.18.0a0+6043bc2 | **Version Difference**: Paired with PyTorch 2.3.0 on Jetson. |

---

## 7. Validation Commands

Safe, non-hardware validation:

```bash
.venv/bin/python -m py_compile \
  src/nodes/node_b.py \
  src/network/mqtt_client.py \
  src/common/config.py \
  src/reid/reid_engine.py \
  src/reid/preprocess.py

.venv/bin/python -c "import src.nodes.node_b; print('Camera B import: PASS')"
PYTHONPATH=. .venv/bin/python tests/test_node_b_passage_e2e.py
PYTHONPATH=. .venv/bin/python tests/environment_check.py
```
