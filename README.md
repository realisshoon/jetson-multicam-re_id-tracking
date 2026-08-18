# Multi-Camera Real-Time Re-ID Tracking System

An end-to-end multi-camera real-time person tracking and Re-Identification (Re-ID) system deploying distributed AI edge nodes on **NVIDIA Jetson Orin Nano**, a central **Main Server** coordinator with SQLite WAL persistence, and a **Django / Daphne** web dashboard.

---

## 1. System Architecture

```text
[ Camera A: Entry ] (Jetson Orin Nano, Port 8000)
    │  • YOLO26n Detection + ByteTrack
    │  • OSNet FP16 Body Re-ID (512-D) + YuNet / SFace Face Re-ID (128-D)
    │  • Boundary Crossing Gate -> MQTT Publish: cctv/events/a/entry
    ▼
[ Central MQTT Broker ] (Mosquitto, Port 1883) ──► [ Main Server ] (Port 8080 / 8091)
    │                                                      │  • Journey lifecycle manager
    ├──────────────────────┬───────────────────────────────┤  • Re-ID gallery aggregator
    ▼                      ▼                               │  • SQLite WAL database
[ Camera B: Passage ]  [ Camera C: Passage ]               │  • Admin DB control API
 (Port 8001)            (Port 8002)                        ▼
    │                      │                         [ Django Web Dashboard ]
    │  • Candidate match   │  • Candidate match      (Daphne ASGI, Port 8000)
    │  • Gallery append    │  • Gallery append         • Live MJPEG streams
    │  • Passage event     │  • Passage event          • Real-time journey map
    └──────────┬───────────┴─────────────────────────► • Person Re-ID review
               ▼
[ Camera D: Destination ] (Jetson Orin Nano, Port 8003)
    │  • Candidate matching against aggregated A + B/C galleries
    │  • Multi-pass confirmation window & temporal guardrails
    │  • Arrival event -> MQTT Publish: cctv/events/d/arrival
    │  • Stranger detection gate -> MQTT Publish: cctv/events/d/detection
    ▼
[ Main Server: Terminal Resolution ]
    • Mark journey COMPLETED (Route: A -> B -> D or A -> C -> D)
    • Publish: cctv/main/journey/completed
```

---

## 2. Core Technologies

- **Object Detection & Tracking**: YOLO26n (`ultralytics==8.4.112`) + ByteTrack (`lap==0.5.12`)
- **Body Re-Identification**: OSNet x0.25 FP16 TensorRT Engine (512-D L2-normalized embeddings)
- **Face Detection & Recognition**: OpenCV DNN with YuNet (detection) and SFace (128-D recognition)
- **Messaging & Communication**: Eclipse Mosquitto MQTT Broker (v2.x, QoS 1) + REST API
- **Persistence & Storage**: SQLite 3 (WAL journal mode) with JSON audit trails
- **Web Dashboard**: Django 5.x + Daphne 4.1 (ASGI WebSocket & MJPEG Proxy) + Redis + WhiteNoise

---

## 3. Repository Structure

```text
├── cctv_main/                   # Central Main Server implementation
│   ├── main_server.py           # Core journey coordinator & MQTT handler
│   ├── api_server.py            # REST API (port 8080)
│   ├── admin_control.py         # Admin DB Control API (port 8091)
│   └── capture_cache.py         # Capture cache manager
├── configs/                     # System & node configuration files
│   ├── mqtt.yaml                # Active MQTT broker configuration
│   ├── mqtt.example.yaml        # MQTT broker template
│   ├── node_d_matching.yaml     # Camera D matching thresholds & guardrails
│   ├── reid_config.yaml         # Re-ID engine configuration
│   └── identity.yaml            # Identity decision parameters
├── docs/                        # System documentation & contracts
│   ├── INTEGRATION_HEADS.md     # Authoritative Golden Commit SHAs
│   ├── ENVIRONMENT.md           # System & dependency matrix
│   ├── PORTS.md                 # Network port contract
│   ├── MQTT_CONTRACT.md         # MQTT topics & JSON schemas
│   └── integration/             # Node integration specifications (A, B, C, D)
├── models/                      # Model binaries manifest
│   └── MANIFEST.md              # Model expected paths, dimensions, SHA-256
├── requirements/                # Multi-environment dependency definitions
│   ├── README.md                # Environment guide
│   ├── jetson-common.txt        # Jetson common direct requirements
│   ├── camera-a.txt             # Camera A requirements
│   ├── camera-b.txt             # Camera B requirements
│   ├── camera-c.txt             # Camera C requirements
│   ├── camera-d.txt             # Camera D requirements
│   ├── main-server.txt          # Main Server requirements
│   ├── web.txt                  # Web Dashboard requirements
│   └── snapshots/               # Exact runtime freeze snapshots
├── scripts/                     # Node execution and service helper scripts
│   ├── run_node_a.sh            # Camera A startup script
│   ├── run_node_b.sh            # Camera B startup script
│   ├── run_node_c.sh            # Camera C startup script
│   ├── run_node_d.sh            # Camera D startup script
│   ├── start_live_stack.ps1     # Main server stack startup (PowerShell)
│   └── stop_live_stack.ps1      # Main server stack shutdown (PowerShell)
├── src/                         # Shared Python modules & nodes
│   ├── common/                  # Shared utilities (config, journey, stranger detection)
│   ├── network/                 # MQTT client & message protocols
│   ├── nodes/                   # Camera node runners (node_a, node_b, node_c, node_d)
│   ├── reid/                    # OSNet Re-ID inference engine & preprocessing
│   └── server/                  # Server protocol adapters & repositories
├── tests/                       # Unit and integration test suites
└── web/                         # Django Admin & Monitoring Web Application
    ├── config/                  # Django project settings & ASGI configuration
    ├── tracking/                # Tracking app, models, views, and migrations
    ├── main_server_worker.py    # Main Server REST API polling worker
    ├── manage.py                # Django management script
    └── requirements-web.txt     # Web direct requirements
```

---

## 4. Golden Sources of Truth

| Role | Branch | Golden Commit SHA |
|---|---|---|
| **Main Server** | `submission/main-server` | `c5b33cf13c96bfebb142ca507de65db36ac25c1c` |
| **Camera A** | `submission/camera-a` | `5a7d4d2841921412112eb394e91f7e0a6d7bfb47` |
| **Camera B** | `submission/camera-b` | `c83530f5019046343e1c53802255ddc113cc0bc8` |
| **Camera C** | `submission/camera-c` | `c7da2eece081d8261169c92d378be0da5b5f3b7f` |
| **Camera D** | `submission/camera-d` | `149422a277ee20e0bce3c7d1a5f58adcac681254` |
| **Web Dashboard** | `reid-admin-web` | `6c34b4805bf760dd01e26893ed5d62e7b4976cba` |

---

## 5. Model Deployment

Before launching nodes, ensure required model weights are placed according to [models/MANIFEST.md](file:///d:/working/final-integration/models/MANIFEST.md):

```text
├── yolo26n.pt                                       # YOLO26n detector (A, B, C, D)
└── models/
    ├── reid/
    │   ├── person_reid_osnet_x0_25.onnx            # Source ONNX model
    │   └── person_reid_osnet_x0_25_fp16.engine     # OSNet TensorRT FP16 engine (A, B, C, D)
    └── face/
        ├── face_detection_yunet_2023mar.onnx        # YuNet face detector (A)
        └── face_recognition_sface_2021dec.onnx      # SFace face recognizer (A)
```

---

## 6. Execution Guide

### Step 1: Start MQTT Broker & Main Server
On the Main Server host (Windows / Linux):
```powershell
# Start MQTT Broker (port 1883)
mosquitto -c configs/mosquitto.main-server.conf

# Start Main Server & REST API
powershell -ExecutionPolicy Bypass -File scripts/start_live_stack.ps1
```

### Step 2: Start Django Web Dashboard
On the Web host:
```powershell
# Start Daphne ASGI server and background worker
powershell -ExecutionPolicy Bypass -File server.ps1 start
```

### Step 3: Start Jetson Camera Nodes
On each respective Jetson board:
```bash
# Camera A (Entry Node, Port 8000)
./scripts/run_node_a.sh

# Camera B (Passage Node, Port 8001)
./scripts/run_node_b.sh

# Camera C (Passage Node, Port 8002)
./scripts/run_node_c.sh

# Camera D (Destination Node, Port 8003)
./scripts/run_node_d.sh
```

---

## 7. Testing & Verification

### Non-Hardware Syntax Verification:
```bash
python -m compileall cctv_main
python -m compileall web
python -m py_compile src/nodes/node_a.py src/nodes/node_b.py src/nodes/node_c.py src/nodes/node_d.py
```

### Protocol & Node Unit Tests:
```bash
python -m unittest discover -s tests -p "test_*.py"
```
