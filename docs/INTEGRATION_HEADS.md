# Integration Heads Record

This document records the exact Golden Commit SHAs and branch references used as the authoritative sources of truth for the Final Multi-Camera Re-ID System integration on branch `submission/final`.

## Golden Sources of Truth

| Role | Branch | Golden Commit SHA | Verified Description |
|---|---|---|---|
| **Main Server** | `submission/main-server` | `c5b33cf13c96bfebb142ca507de65db36ac25c1c` | Central coordinator, REST API (8080), Admin DB API (8091), SQLite WAL persistence, revisit diagnostics |
| **Camera A** | `submission/camera-a` | `5a7d4d2841921412112eb394e91f7e0a6d7bfb47` | Entry node (8000), YOLO26n + ByteTrack + OSNet FP16 Re-ID + YuNet/SFace Face Re-ID, entry event publish |
| **Camera B** | `submission/camera-b` | `c83530f5019046343e1c53802255ddc113cc0bc8` | Intermediate passage node (8001), YOLO26n + ByteTrack + OSNet FP16 Re-ID, candidate matching & passage publish |
| **Camera C** | `submission/camera-c` | `c7da2eece081d8261169c92d378be0da5b5f3b7f` | Intermediate passage node (8002), YOLO26n + ByteTrack + OSNet FP16 Re-ID, candidate matching & passage publish |
| **Camera D** | `submission/camera-d` | `149422a277ee20e0bce3c7d1a5f58adcac681254` | Destination node (8003), YOLO26n + ByteTrack + OSNet FP16 Re-ID, boundary crossing, arrival publish, stranger detection |
| **Web Dashboard** | `reid-admin-web` | `6c34b4805bf760dd01e26893ed5d62e7b4976cba` | Django 5.x + Daphne ASGI dashboard, real-time tracking, journey review, database proxy |

---

## Final Integrated Target
- **Target Branch**: `submission/final`
- **Base Commit**: `c5b33cf13c96bfebb142ca507de65db36ac25c1c` (Main Server Golden)
- **Integration Strategy**: Explicit component extraction and non-destructive reconciliation across roles.
