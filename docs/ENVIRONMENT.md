# Python & System Environment

## Project standard

- Supported Python: 3.10.x
- Reference environment: Python 3.10.11 / Python 3.10.12
- All team members must create this project's virtual environment with Python 3.10.
- Existing system Python installations do not need to be removed.

## Jetson AI nodes (Camera A, B, C, D)

- Platform: NVIDIA Jetson Orin Nano / JetPack 5.x ~ 6.x
- Python: 3.10.x
- Dependencies: `requirements.txt`
- JetPack-provided CUDA, TensorRT, Torch and OpenCV must not be replaced by generic PyPI builds.
- Models specification: `models/MANIFEST.md`

## Windows / Linux Central Main Server

- Platform: Windows 11 Pro 64-bit / Ubuntu 22.04 LTS
- Python: 3.10.11
- SQLite: 3.40.1 (built-in Python sqlite3, WAL journal mode)
- MQTT Broker: Eclipse Mosquitto v2.x (TCP port 1883)
- Runtime Direct Dependencies: `requirements-server.txt`
- Exact Environment Snapshot: `snapshots/main-server.freeze.txt`

## Django Admin Web

- Python: 3.10.x
- Dependencies: `web/requirements-web.txt`
- Services: Redis (channel layer), SQLite / Central Main REST API proxy

## Important Rules

- Virtual environments (`.venv*`) are local and must not be committed.
- Switching Git branches does not automatically recreate or change a virtual environment.
- Recreate the environment when the Python major/minor version changes.
- Do not copy a virtual environment between Windows and Jetson.
- Never commit model binaries (`.engine`, `.onnx`, `.pt`) or live database files (`.db`).
