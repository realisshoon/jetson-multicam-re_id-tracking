# System Environment Specification

This document provides the authoritative system, runtime, and dependency matrix across all roles in the CCTV Multi-Camera Tracking System.

---

## 1. System Matrix

| Role | OS / Platform | Python | CUDA | cuDNN | TensorRT | PyTorch | OpenCV | Primary Web / REST Port |
|---|---|---|---|---|---|---|---|---|
| **Camera A** | Linux Ubuntu 22.04 LTS (Jetson Orin) | 3.10.12 | 12.6 | 9.3 | 10.3.0 | 2.8.0 | 4.11.0.86 | 8000 (MJPEG Stream) |
| **Camera B** | Linux Ubuntu 22.04 LTS (Jetson Orin) | 3.10.12 | 12.6 | 9.3 | 10.3.0 | 2.3.0 | 4.11.0.86 | 8001 (MJPEG Stream) |
| **Camera C** | Linux Ubuntu 22.04 LTS (Jetson Orin) | 3.10.12 | 12.6 | 9.3 | 10.3.0 | 2.3.0 | 4.11.0.86 | 8002 (MJPEG Stream) |
| **Camera D** | Linux Ubuntu 22.04 LTS (Jetson Orin) | 3.10.12 | 12.6 | 9.3 | 10.3.0 | 2.3.0 | 4.8.0.76 | 8003 (MJPEG Stream) |
| **Main Server** | Windows 11 Pro 64-bit / Ubuntu 22.04 | 3.10.11 | N/A | N/A | N/A | N/A | N/A | 8080 (REST), 8091 (Admin API) |
| **Web Dashboard** | Windows 11 / Linux Ubuntu 22.04 | 3.10.x | N/A | N/A | N/A | N/A | 4.9+ | 8000 (Daphne ASGI Dashboard) |

---

## 2. System Packages & Services

System-level packages must be provisioned and managed at the OS level:

- **JetPack / L4T**: JetPack 6.x / L4T R36.5.0 (`nvidia-l4t-core`) on Jetson Orin nodes.
- **CUDA & cuDNN**: CUDA 12.6, cuDNN 9.3.
- **TensorRT**: 10.3.0.30 (`libnvinfer10`, Python binding `tensorrt==10.3.0`).
- **Mosquitto MQTT Broker**: Eclipse Mosquitto v2.x (TCP port 1883) hosted on Main Server / Network Hub.
- **SQLite**: SQLite 3.40.1+ with Write-Ahead Logging (`PRAGMA journal_mode=WAL`).

---

## 3. Direct Python Dependencies by Role

| Package | Camera A | Camera B | Camera C | Camera D | Main Server | Web Dashboard |
|---|---|---|---|---|---|---|
| `numpy` | 1.26.4 | 1.26.4 | 1.26.4 | 1.26.4 | 1.26.4 | 1.26.4 |
| `paho-mqtt` | 2.1.0 | 2.1.0 | 2.1.0 | 2.1.0 | 2.1.0 | >=2.1,<3.0 |
| `PyYAML` | 6.0.2 | 6.0.2 | 6.0.2 | 6.0.2 | 6.0.2 | N/A |
| `ultralytics` | 8.4.112 | 8.4.112 | 8.4.112 | 8.4.112 | N/A | N/A |
| `lap` | 0.5.12 | 0.5.12 | 0.5.12 | 0.5.12 | N/A | N/A |
| `Pillow` | N/A | N/A | N/A | N/A | 12.0.0 | >=10.0 |
| `Django` | N/A | N/A | N/A | N/A | N/A | >=5.0,<6.0 |
| `daphne` | N/A | N/A | N/A | N/A | N/A | >=4.1 |
| `redis` | N/A | N/A | N/A | N/A | N/A | >=5.0 |
| `whitenoise`| N/A | N/A | N/A | N/A | N/A | >=6.6 |
| `requests`  | N/A | N/A | N/A | N/A | N/A | >=2.31 |

---

## 4. Operational Rules

1. **Virtual Environments**: Always use separate Python 3.10 virtual environments per machine. Do not commit or share virtual environment directories (`.venv`, `venv`).
2. **Binary Artifacts**: Do not commit model weights (`.pt`), TensorRT engines (`.engine`), ONNX models (`.onnx`), or live databases (`*.db`, `*.sqlite3`).
3. **Jetson Packages**: Do not replace JetPack PyTorch, torchvision, or OpenCV with standard PyPI wheels on Jetson boards.
