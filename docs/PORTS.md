# Port Contract Specification

This document defines the authoritative network port allocations, service endpoints, and protocols across all components in the CCTV Multi-Camera Tracking System.

---

## 1. System Port Allocation Table

| Service / Role | Default Port | Protocol | Host / Binding | Description |
|---|---|---|---|---|
| **Camera A** | `8000` | HTTP / MJPEG | `0.0.0.0:8000` | Live MJPEG annotated stream & capture inspection |
| **Camera B** | `8001` | HTTP / MJPEG | `0.0.0.0:8001` | Live MJPEG annotated stream (`/stream`, `/`) |
| **Camera C** | `8002` | HTTP / MJPEG | `0.0.0.0:8002` | Live MJPEG annotated stream (`/stream`, `/`) |
| **Camera D** | `8003` | HTTP / MJPEG | `0.0.0.0:8003` | Live MJPEG annotated stream (`/stream`, `/captures/D/...`) |
| **MQTT Broker** | `1883` | MQTT / TCP | `10.10.20.33:1883` | Eclipse Mosquitto message broker for inter-node events |
| **Main Server REST API** | `8080` | HTTP / REST | `0.0.0.0:8080` | Central tracking queries (`/api/v1/journeys`, `/api/v1/persons`, etc.) |
| **Main Server Admin API**| `8091` | HTTP / REST | `127.0.0.1:8091` | Admin DB management (`/admin/database/status`, `/backup`, `/reset/*`) |
| **Django Web Dashboard** | `8000` | HTTP / WebSocket | `0.0.0.0:8000` | Daphne ASGI server for Admin UI and WebSocket real-time updates |

---

## 2. Port Conflict Prevention

> [!NOTE]
> - Camera A, B, C, and D bind to dedicated distinct ports (`8000`, `8001`, `8002`, `8003`) respectively when running on separate Jetson boards or local test environments.
> - Main Server REST API (`8080`) and Admin Control API (`8091`) operate on separate dedicated ports to prevent external exposure of administrative database reset tokens.
> - Django Web Dashboard defaults to port `8000` on its host machine (e.g. `10.10.20.26`) or can be configured via `run_dev.sh` / `server.ps1`.
