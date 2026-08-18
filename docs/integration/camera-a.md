# Camera A integration contract

This is the handoff contract for `src.nodes.node_a` at the Camera A final-freeze
stage. It records current behavior; it does not redefine thresholds or message
contracts.

## Role and production flow

Camera A is node ID `A` and owns entrance detection and initial identity feature
creation:

```text
YOLO person detection
  -> ByteTrack local tracking
  -> body crop selection (top 3)
  -> OSNet TensorRT 512-D embeddings
  -> YuNet face detection (top 3)
  -> SFace 128-D embeddings
  -> A ENTRY MQTT event
```

The current local production change arms a track after its bounding-box center
enters the ENTRY zone and emits ENTRY only when that armed track exits the zone
through its left or bottom boundary. This replaces the older single-line
crossing rule and is intentionally frozen without refactoring.

## Start command

From the repository root, with the approved Camera A environment activated:

```bash
./scripts/run_node_a.sh
```

The script changes to the repository root and executes
`python3 -m src.nodes.node_a`. The module has no argument parser, so
`python3 -m src.nodes.node_a --help` is not a safe help-only command; use a
plain import test for non-camera validation.

## Runtime endpoints and identifiers

| Item | Current contract |
|---|---|
| Node ID | `A` (literal in MQTT payloads) |
| ENTRY publisher client ID | `camera-a` |
| Response/timing client ID | `camera_a_response_<8 random hex characters>` |
| MQTT host | `10.10.20.33` current source/default config |
| MQTT port / QoS | 1883 / 1 |
| Publish | `cctv/events/a/entry` (`ENTRY`) |
| Publish | `cctv/events/a/timing` (`NODE_TIMING`) |
| Subscribe | `cctv/responses/a/entry` (`ENTRY_RESULT`) |
| Web bind | `0.0.0.0:8000` |
| Web routes | `/`, `/stream`, `/captures/body/...`, `/captures/face/...` |
| Camera | `/dev/video0`, V4L2, MJPEG, 1280x720 at requested 30 FPS |
| Frame transform | horizontal flip enabled; contrast 1.02; brightness +8 |

The ENTRY publisher obtains broker host, port, and QoS through
`src.common.config`: `JETSON_MQTT_CONFIG`, then `configs/mqtt.yaml`, then
`configs/mqtt.example.yaml`. The response/timing client currently uses the
`MQTT_HOST` and `MQTT_PORT` constants in `src/nodes/node_a.py`. These two paths
must resolve to the same broker in deployment. This freeze records the existing
behavior and does not refactor it.

`JETSON_MQTT_CONFIG` is optional, not required when the example/default config
is appropriate. If used, point it at a machine-local YAML file with this shape:

```yaml
broker:
  host: <MQTT_BROKER_HOST>
  port: 1883
  qos: 1
```

No MQTT username, password, token, or `.env` value is committed. If the target
broker later requires credentials, provision them outside Git and update the
integration contract through an approved change rather than placing secrets in
this document.

## Models

| Logical model | Expected path | Runtime use |
|---|---|---|
| YOLO26n | `yolo26n.pt` | person detection feeding ByteTrack |
| OSNet x0.25 FP16 engine | `models/reid/person_reid_osnet_x0_25_fp16.engine` | 512-D body embedding |
| YuNet 2023mar | `models/face/face_detection_yunet_2023mar.onnx` | face detection and landmarks |
| SFace 2021dec | `models/face/face_recognition_sface_2021dec.onnx` | 128-D face embedding |

The portable OSNet ONNX source is needed only when building/provisioning the
TensorRT engine, not by the Camera A inference path. See `models/MANIFEST.md`
for sizes, hashes, tensor contracts, and runtime compatibility.

## Frozen runtime settings

| Setting | Value |
|---|---:|
| YOLO class filter | person (`classes=[0]`) |
| YOLO confidence | 0.50 |
| YOLO IoU | 0.50 |
| Tracker | `bytetrack.yaml`, persistent IDs |
| ENTRY zone X ratio | 0.65 to 1.00 |
| ENTRY zone Y ratio | 0.00 to 0.62 |
| Body top K | 3 |
| Body minimum frame gap | 8 |
| Body post-entry grace | 0.60 seconds |
| Face score threshold | 0.60 |
| Face top K | 3 |
| Face check interval | 2 frames |
| Face minimum frame gap | 6 |
| Face post-entry grace | 0.35 seconds |
| Face minimum size | 24 pixels |
| Face minimum sharpness | 10.0 |
| Face minimum frontal score | 0.20 |
| Face fallback upscale | 2.0 |
| Track state timeout | 8.0 seconds |

These are observations, not recommendations. Integration work must not change
them without separate calibration and approval.

## Runtime outputs

Camera A writes captures and logs below the repository root:

```text
outputs/captures/A/
outputs/captures/A_face/
logs/node_a_entry_central.csv
```

These paths, Python caches, databases, and model binaries are excluded from Git.

## Safe validation

Safe checks that do not start the physical camera are:

```bash
bin/python -m py_compile \
  src/nodes/node_a.py \
  src/network/mqtt_client.py \
  src/common/config.py \
  src/reid/reid_engine.py \
  src/reid/preprocess.py

bin/python -c "import src.nodes.node_a; print('Camera A import: PASS')"
bin/python tests/environment_check.py
```

Full validation additionally requires an exposed Jetson GPU, `/dev/video0`, the
four runtime model files, the MQTT broker, and a free TCP port 8000.
