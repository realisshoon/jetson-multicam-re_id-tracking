# Camera A environment freeze

This document records the Camera A host as audited on 2026-08-18 (Asia/Seoul).
It is an environment handoff, not an installation script. No package was
installed, upgraded, removed, or replaced during the audit.

## Runtime selected for the freeze

The machine contains more than one Python environment. The runtime whose
packages match the repository's existing dependency checks is the machine-local
virtual environment rooted at `/home/aidl/work/pj`:

| Item | Audited value |
|---|---|
| Repository | `/home/aidl/work/pj` |
| Runtime Python | `/home/aidl/work/pj/bin/python` |
| Python | 3.10.12 |
| pip | 22.0.2 |
| Architecture | aarch64 |
| Kernel | `5.15.185-tegra` |
| Full Python snapshot | `requirements/snapshots/camera-a.freeze.txt` |

The unactivated shell resolved `python3` to `/usr/bin/python3` and `pip` to
`/usr/bin/pip`. That interpreter does not contain PyTorch, Ultralytics, or
paho-mqtt and cannot run Camera A. `scripts/run_node_a.sh` deliberately uses
`python3` from `PATH`, so the intended environment must be activated first.

The current checkout contains its virtual environment inside the repository
root, but `bin/`, `lib/`, `share/`, and `pyvenv.cfg` are ignored and are not
portable Git content. A clone must create or select a compatible environment;
it must not expect those local directories to arrive from Git.

## Python packages used by Camera A

`requirements/jetson.txt` is the curated direct/runtime dependency list. The
following versions were imported successfully from the selected runtime:

| Import/runtime role | Distribution | Version | Provider in audited runtime |
|---|---|---:|---|
| arrays and preprocessing | numpy | 1.26.4 | virtual environment |
| V4L2/image/DNN/face APIs | opencv-python | 4.11.0.86 | virtual environment |
| MQTT | paho-mqtt | 2.1.0 | virtual environment |
| YAML config | PyYAML | 6.0.2 | virtual environment |
| YOLO and ByteTrack interface | ultralytics | 8.4.112 | virtual environment |
| ByteTrack assignment backend | lap | 0.5.12 | virtual environment |
| CUDA tensor bridge | torch | 2.8.0 | virtual environment; source provenance unavailable |
| Ultralytics runtime companion | torchvision | 0.23.0 | virtual environment |
| OSNet engine runtime | tensorrt | 10.3.0 | `/usr/lib/python3.10/dist-packages` |

ONNX and ONNX Runtime are not imported by Camera A at runtime and were not
installed in the audited environment. The OSNet ONNX file is an engine-build
artifact; inference uses the TensorRT `.engine`. YuNet and SFace ONNX files are
loaded directly by OpenCV DNN.

Do not treat `requirements/snapshots/camera-a.freeze.txt` as an install file.
It includes transitive packages and CUDA-related Python packages for comparison
only. No editable or local-path package appears in the selected runtime's
snapshot. A second machine-local environment at `/home/aidl/work/venv` did
contain local-path NVIDIA wheels, but it was not selected because it lacked
paho-mqtt and therefore could not import Camera A.

## Jetson and system dependencies

| Component | Audited value | Provisioning note |
|---|---|---|
| JetPack meta package | 6.2.3+b81 | Install as a Jetson image/system package, not with pip. |
| L4T | R36.5.0 (`nvidia-l4t-core 36.5.0`) | Must match the target board image. |
| CUDA toolkit | 12.6 (`nvcc 12.6.68`) | JetPack/system package. |
| cuDNN | 9.3.0.75 | `libcudnn9-cuda-12` system package. |
| TensorRT runtime | 10.3.0.30 | `libnvinfer10` and TensorRT system packages. |
| OpenCV system install | 4.8.0 | Coexists with runtime pip OpenCV 4.11.0. |
| Mosquitto broker/clients | 2.0.11 | System service/package; not a Python requirement. |
| SQLite runtime | 3.37.2 | Available through Python stdlib; `sqlite3` CLI was absent. |

The selected OpenCV 4.11.0 build reports `GStreamer: NO`, but it provides
`FaceDetectorYN` and `FaceRecognizerSF`. Camera A currently opens `/dev/video0`
with the V4L2 backend and does not require GStreamer for that configured source.

### Audit-session hardware limitation

`torch.cuda.is_available()` returned `False`; `nvidia-smi` could not communicate
with the driver; and `/dev/nvmap`, `/dev/nvhost-ctrl-gpu`, and `/dev/video0` were
not exposed to the audit session. Consequently, CUDA/TensorRT inference and the
physical camera could not be executed here. The installed versions above were
verified, but a privileged on-device session must complete the hardware checks
before declaring the freeze fully reproducible.

## Reproduction contract

1. Provision a compatible Jetson image with JetPack/L4T, CUDA, cuDNN, TensorRT,
   and the TensorRT Python binding versions recorded above.
2. Create or select a Python 3.10 environment. Do not blindly install a generic
   PyPI PyTorch build: obtain the NVIDIA/Jetson build compatible with JetPack
   6.2.3 and verify that it provides PyTorch 2.8.0 with CUDA 12.6 behavior
   compatible with this host. The audited package metadata does not retain the
   original download URL, so the wheel must come from the approved team bundle
   or NVIDIA source.
3. Review `requirements/jetson.txt`, then install only the ordinary Python
   packages after confirming they will not replace the chosen Jetson PyTorch,
   TensorRT, or OpenCV stack.
4. Provision the model files listed in `models/MANIFEST.md`; never commit them.
5. Provide MQTT configuration as described in
   `docs/integration/camera-a.md`. Do not commit credentials or a real `.env`.
6. Activate the selected environment so `python3` resolves to it, then run from
   the repository root:

   ```bash
   ./scripts/run_node_a.sh
   ```

7. Before production, verify `torch.cuda.is_available()` is `True`, the TensorRT
   engine deserializes, `/dev/video0` yields 1280x720 MJPEG frames at the
   required rate, MQTT publish/subscribe succeeds, and port 8000 is available.

## Audit commands

The freeze used these non-mutating commands:

```bash
python3 --version
which python3
which pip
python3 -m pip --version
python3 -m pip freeze

bin/python --version
bin/python -m pip --version
bin/python -m pip freeze

uname -a
cat /etc/nv_tegra_release
nvcc --version
nvidia-smi
dpkg -l | grep -E "tensorrt|cuda|cudnn"
```
