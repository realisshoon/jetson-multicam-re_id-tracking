# Multi-Environment Requirements Specification

The CCTV Multi-Camera Tracking System spans heterogeneous platforms (NVIDIA Jetson Orin boards, Windows/Linux Main Server, and Django Web Dashboard). A single global `pip freeze` is not applicable due to differing hardware architectures, JetPack system packages, and framework requirements.

---

## Directory Structure

```text
requirements/
    README.md                  # This documentation
    jetson-common.txt          # Verified direct dependencies common across Jetson nodes
    camera-a.txt               # Camera A entry node direct dependencies
    camera-b.txt               # Camera B passage node direct dependencies
    camera-c.txt               # Camera C passage node direct dependencies
    camera-d.txt               # Camera D destination node direct dependencies
    main-server.txt            # Main Server direct dependencies (Windows / Linux)
    web.txt                    # Web Dashboard direct dependencies (Django / Daphne)

    snapshots/
        camera-a.freeze.txt    # Exact runtime freeze snapshot from Camera A Jetson board
        camera-b.freeze.txt    # Exact runtime freeze snapshot from Camera B Jetson board
        camera-c.freeze.txt    # Exact runtime freeze snapshot from Camera C Jetson board
        camera-d.freeze.txt    # Exact runtime freeze snapshot from Camera D Jetson board
        main-server.freeze.txt # Exact runtime freeze snapshot from Windows Main Server
```

---

## Environment Separation & Rationale

### 1. Jetson AI Edge Nodes (Camera A, B, C, D)
- **Target OS**: Ubuntu 22.04 LTS (aarch64) / JetPack 6.x / L4T R36.5.0
- **Python**: 3.10.12
- **Common Python Packages**:
  - `numpy==1.26.4`
  - `paho-mqtt==2.1.0`
  - `PyYAML==6.0.2`
  - `ultralytics==8.4.112`
  - `lap==0.5.12`
- **Platform Packages (System / JetPack / Wheel)**:
  - `tensorrt==10.3.0`
  - `torch==2.3.0` (JetPack official wheel) / `torch==2.8.0` (Camera A custom build)
  - `torchvision==0.18.0a0+6043bc2` / `torchvision==0.23.0`
  - `opencv-python==4.11.0.86` / `opencv-python==4.8.0.76` (Camera D)
  > [!WARNING]
  > Do not install standard generic PyPI wheels for `torch`, `torchvision`, `tensorrt`, or `opencv` on Jetson hardware, as this can overwrite hardware-accelerated CUDA and GStreamer bindings.

### 2. Main Server (Central Coordinator)
- **Target OS**: Windows 11 Pro 64-bit / Ubuntu 22.04 LTS (x86_64)
- **Python**: 3.10.11
- **Direct Dependencies**:
  - `numpy==1.26.4`
  - `paho-mqtt==2.1.0`
  - `PyYAML==6.0.2`
  - `Pillow==12.0.0`
- **System Services**:
  - Eclipse Mosquitto MQTT Broker (port 1883)
  - SQLite 3.40+ (Python built-in with WAL mode)

### 3. Web Dashboard (Django Admin & Monitoring)
- **Target OS**: Windows / Linux
- **Python**: 3.10.x
- **Framework**: Django 5.x, Daphne 4.1 (ASGI), Redis 5.x, WhiteNoise 6.6
- **Source of Truth**: `web/requirements-web.txt`

---

## Installation Commands

### On Jetson Edge Nodes:
```bash
# In Jetson node virtualenv
pip install -r requirements/camera-<a|b|c|d>.txt
```

### On Main Server:
```bash
# In Main server virtualenv
pip install -r requirements/main-server.txt
```

### On Web Server:
```bash
# In Web dashboard virtualenv
pip install -r web/requirements-web.txt
```
