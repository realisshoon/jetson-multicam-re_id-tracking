# Camera C Integration Contract

This document specifies the handoff contract and environment state for `src.nodes.node_c` at the Camera C final-freeze milestone. It accurately reflects the current operating behavior, message schemas, and environment settings without altering thresholds or runtime logic.

---

## 1. Role and Production Flow

Camera C operates as intermediate Node ID `C` in the multi-camera tracking pipeline (Route A -> C -> D):

```text
Central Broker (cctv/candidates/c)
  -> Candidate Queue & Storage (Journey ID / Person UID / A Gallery 512-D)
  -> Camera Stream (/dev/video0, 1280x720 @ 30 FPS, V4L2)
  -> YOLO Person Detection (yolo26n.pt, conf=0.50, iou=0.50)
  -> ByteTrack Local Multi-Object Tracking (bytetrack.yaml)
  -> Body Crop Extraction & Preprocessing (256x128 BGR Normalized)
  -> OSNet FP16 TensorRT 512-D Embedding Extraction
  -> Multi-frame Candidate Similarity Matching (Cosine similarity vs A Gallery)
  -> Temporal Window Aggregation & High-Quality Gallery Selection (Target: 2, Quality >= 0.70)
  -> C PASSAGE Event Validation (Best >= 0.75, Top-K >= 0.68, Combined >= 0.72, Consistency >= 2)
  -> C PASSAGE Wire Payload Invariant Verification & MQTT Publish (cctv/events/c/passage)
  -> Route Update: ["A", "C"], Next: ["D"] -> Main Server
```

---

## 2. Start Command

From the repository root, using the approved Python environment:

```bash
./scripts/run_node_c.sh
```

Or directly:

```bash
python3 -m src.nodes.node_c
```

---

## 3. Runtime Endpoints and Identifiers

| Item | Audited Specification |
|---|---|
| Node ID | `C` |
| MQTT Client ID | `camera-c` |
| MQTT Host | `10.10.20.33` (via `configs/mqtt.yaml` / `JETSON_MQTT_CONFIG` / `configs/mqtt.example.yaml`) |
| MQTT Port | `1883` |
| MQTT QoS | `1` |
| Candidate Subscribe Topic | `cctv/candidates/c` |
| Passage Publish Topic | `cctv/events/c/passage` |
| Timing Topic | None / N/A in C runtime |
| Control Topic | None / N/A in C runtime |
| Web Server Bind | `0.0.0.0:8002` |
| Web Routes | `/`, `/stream` |
| Camera Device | `/dev/video0` (V4L2, 1280x720, requested 30 FPS, MJPEG) |
| Frame Transforms | Horizontal flip = True, Contrast alpha = 1.02, Brightness beta = 8 |

---

## 4. Models and Paths

| Model | Path | Purpose | Size / Status |
|---|---|---|---|
| YOLO26n | `yolo26n.pt` | Person detection feeding ByteTrack | 5,544,453 bytes (Verified) |
| OSNet x0.25 FP16 Engine | `models/reid/person_reid_osnet_x0_25_fp16.engine` | 512-D TensorRT body Re-ID embedding | 1,656,508 bytes (Verified) |

*Note: Camera C does not perform face recognition; YuNet and SFace models are not required for C runtime.*

---

## 5. Frozen Runtime Settings

| Parameter | Frozen Value | Note |
|---|---:|---|
| YOLO Confidence / IoU | 0.50 / 0.50 | Person class (`classes=[0]`) |
| Tracker Backend | `bytetrack.yaml` | `lap==0.5.12` |
| Match Threshold (`MATCH_THRESHOLD`) | 0.70 | Minimum similarity to begin match verification |
| Match Margin (`MATCH_MARGIN`) | 0.05 | Margin over runner-up candidate |
| Match Confirmations (`MATCH_CONFIRMATIONS`) | 3 | Consecutive match count required |
| Verify Threshold (`VERIFY_THRESHOLD`) | 0.55 | Tracking continuity threshold |
| Verify Failure Limit (`VERIFY_FAILURE_LIMIT`) | 2 | Max consecutive verification misses |
| Re-ID Interval Frames (`REID_INTERVAL_FRAMES`) | 3 | Inference interval for active tracks |
| Re-ID History Size (`REID_HISTORY_SIZE`) | 5 | Moving average window for body embedding |
| C Gallery Target (`C_GALLERY_TARGET`) | 2 | Required number of C-node gallery embeddings |
| C Gallery Max (`C_GALLERY_MAX`) | 2 | Maximum number of C-node gallery embeddings |
| Min Quality Threshold (`C_PASSAGE_MIN_QUALITY`) | 0.70 | Minimum crop quality for passage inclusion |
| Passage Min Re-ID Samples (`PASSAGE_MIN_REID_SAMPLES`) | 2 | Minimum high-quality C samples in wire gallery |
| Passage Min Best Score (`PASSAGE_MIN_BEST_SCORE`) | 0.75 | Gate for valid passage event |
| Passage Min Top-K Score (`PASSAGE_MIN_TOPK_SCORE`) | 0.68 | Gate for valid passage event (Top-K=3) |
| Passage Min Combined Score (`PASSAGE_MIN_COMBINED_SCORE`) | 0.72 | Weighted combination score (0.45 best + 0.55 top-k) |
| Passage Min Consistency Count (`PASSAGE_MIN_CONSISTENT_COUNT`) | 2 | Frames meeting consistency threshold (0.72) |
| Temporal Window Size (`TEMPORAL_WINDOW_SIZE`) | 3 | Size of temporal observation window |
| Temporal Candidate Bank Max (`TEMPORAL_CANDIDATE_BANK_MAX`) | 6 | Candidate bank capacity |
| Gallery Min Frame Gap (`GALLERY_MIN_FRAME_GAP`) | 10 | Minimum frame distance between gallery samples |
| Gallery Duplicate Threshold (`GALLERY_DUPLICATE_THRESHOLD`) | 0.999 | Duplicate rejection threshold |
| Candidate Timeout (`CANDIDATE_TIMEOUT_SECONDS`) | 300.0s | Max wait time for candidate arrival |
| Track Lost Grace Frames (`TRACK_LOST_GRACE_FRAMES`) | 20 | Frames to retain lost track before release |
| Wire Score Tolerance (`WIRE_SCORE_TOLERANCE`) | 1e-6 | Strict wire invariant tolerance |
| Decision Formula Version | `MAIN_WIRE_V1` | Cross-node scoring compatibility |

---

## 6. Environment and Dependency Comparison

### System Environment
- **Host / OS**: Linux ubuntu 5.15.185-tegra aarch64 (Jetson Orin Nano)
- **JetPack / L4T**: JetPack 6.2.3+b81 (`nvidia-jetpack`), L4T R36.5.0
- **CUDA Toolkit**: 12.6 (`nvcc 12.6.68`)
- **cuDNN**: 9.3.0.75 (`libcudnn9-cuda-12`)
- **TensorRT**: 10.3.0.30 (`libnvinfer10`, Python binding `tensorrt==10.3.0`)
- **Python**: 3.10.12

### Dependency Delta Table vs Golden Branches

| Package | Camera A (`requirements/jetson.txt`) | Camera B (`camera-b.freeze.txt`) | Camera C (`camera-c.freeze.txt`) | Status / Delta Classification |
|---|---|---|---|---|
| `numpy` | 1.26.4 | 1.26.4 | 1.26.4 | **Exact Match across A, B, C** |
| `opencv-python` | 4.11.0.86 | 4.11.0.86 | 4.11.0.86 | **Exact Match across A, B, C** |
| `paho-mqtt` | 2.1.0 | 2.1.0 | 2.1.0 | **Exact Match across A, B, C** |
| `PyYAML` | 6.0.2 | 6.0.2 | 6.0.2 | **Exact Match across A, B, C** |
| `ultralytics` | 8.4.112 | 8.4.112 | 8.4.112 | **Exact Match across A, B, C** |
| `lap` | 0.5.12 | 0.5.12 | 0.5.12 | **Exact Match across A, B, C** |
| `tensorrt` | 10.3.0 | 10.3.0 | 10.3.0 | **Exact Match across A, B, C** |
| `torch` | 2.8.0 | 2.3.0 | 2.3.0 | **Match with B** (JetPack 6 official wheel `torch-2.3.0-cp310-cp310-linux_aarch64.whl` on Jetson hardware) |
| `torchvision` | 0.23.0 | 0.18.0a0+6043bc2 | 0.18.0a0+6043bc2 | **Match with B** (JetPack 6 official wheel paired with PyTorch 2.3.0) |

---

## 7. Runtime Outputs and Persistence

Camera C writes captures and runtime logs to:

```text
outputs/captures/C/
logs/node_c_candidates.csv
logs/node_c_matches.csv
logs/node_c_passages.csv
logs/node_c_passage_diagnostics.jsonl
logs/revisit/<REVISIT_RUN_ID>/camera_c_revisit.jsonl
```

All captures, runtime logs, SQLite databases, Python cache files, and model binary files are excluded from Git.

---

## 8. Validation Commands

Safe, non-hardware validation:

```bash
python3 -m py_compile \
  src/nodes/node_c.py \
  src/network/mqtt_client.py \
  src/common/config.py \
  src/reid/reid_engine.py \
  src/reid/preprocess.py

python3 -c "import src.nodes.node_c; print('Camera C import: PASS')"
PYTHONPATH=. python3 tests/test_node_c_passage_e2e.py
PYTHONPATH=. python3 tests/environment_check.py
```
