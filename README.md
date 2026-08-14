# Jetson Multi-Camera Re-ID Tracking

Jetson A/B/D가 동일한 저장소를 사용하면서 각자의 역할만 실행하는 멀티카메라
Re-ID CCTV 프로젝트입니다. Windows Main Server는 별도 환경에서 실행되며 Jetson
노드와 MQTT로 통신합니다.

## Architecture

```text
Jetson A (ENTRY)
  -> cctv/events/a/entry
  -> Windows Main Server
  -> cctv/candidates/b
  -> Jetson B (PASSAGE)
  -> cctv/events/b/passage
  -> Windows Main Server
  -> cctv/candidates/d
  -> Jetson D (ARRIVAL)
  -> cctv/events/d/arrival
  -> Windows Main Server
```

모든 노드는 중앙 MQTT Broker `10.10.20.33:1883`, QoS 1을 사용합니다.
Re-ID embedding은 JSON 내부의 512개 float 배열로 전달됩니다.

| Node | 역할 | Subscribe | Publish | MQTT client ID |
|---|---|---|---|---|
| A | 출입 감지 및 최초 embedding 생성 | `cctv/responses/a/entry` | `cctv/events/a/entry` | `camera-a` (`camera-a-response-*`는 응답 전용) |
| B | A 후보 매칭 및 passage gallery 생성 | `cctv/candidates/b` | `cctv/events/b/passage` | `camera-b` |
| D | A+B gallery 최종 매칭 및 arrival 전송 | `cctv/candidates/d` | `cctv/events/d/arrival` | `camera-d` |

## Clone

각 Jetson에서 저장소 전체를 clone합니다. 보드마다 파일을 삭제하거나 다른 브랜치를
사용하지 않습니다.

```bash
git clone https://github.com/realisshoon/jetson-multicam-re_id-tracking.git
cd jetson-multicam-re_id-tracking
```

## Jetson 환경 확인

이 저장소는 CUDA, TensorRT, PyTorch, torchvision, OpenCV를 자동 설치하거나
교체하지 않습니다. 먼저 현재 JetPack 환경을 확인합니다.

```bash
python3 --version
python3 -c "import torch; print(torch.__version__, torch.cuda.is_available())"
python3 -c "import cv2; print(cv2.__version__)"
python3 -c "import tensorrt; print(tensorrt.__version__)"
python3 tests/environment_check.py
```

`requirements.txt`에는 프로젝트가 추가로 사용하는 Python 패키지가 정리되어
있습니다. 설치 전에 기존 Jetson용 torch/OpenCV/TensorRT가 정상인지 확인하십시오.
일반 PyPI의 torch, torchvision 또는 OpenCV로 강제 교체하지 마십시오.

## 모델 배치

모델 바이너리는 Git에 포함하지 않습니다. 팀에서 승인한 모델 묶음을 받아 다음
경로에 배치합니다.

```text
yolo26n.pt
models/reid/person_reid_osnet_x0_25.onnx
models/reid/person_reid_osnet_x0_25_fp16.engine
models/face/face_detection_yunet_2023mar.onnx
```

정확한 크기와 SHA-256, 보드별 필요 파일은
[`models/MANIFEST.md`](models/MANIFEST.md)를 확인하십시오. TensorRT engine은
JetPack/CUDA/TensorRT/GPU 환경에 의존하므로 호환되지 않는 장치의 engine을 그대로
복사하지 마십시오. 모델이 없으면 실행 시작 시 누락된 전체 경로를 출력합니다.

```bash
sha256sum yolo26n.pt \
  models/reid/person_reid_osnet_x0_25.onnx \
  models/reid/person_reid_osnet_x0_25_fp16.engine \
  models/face/face_detection_yunet_2023mar.onnx
```

## MQTT 설정

기본 template은 `configs/mqtt.example.yaml`이며 현재 중앙 Broker 설정이 들어
있습니다. 보드별 로컬 설정이 필요하면 다음처럼 복사합니다.

```bash
cp configs/mqtt.example.yaml configs/mqtt.yaml
```

`configs/mqtt.yaml`은 Git에서 제외됩니다. 다른 위치의 설정은 환경 변수로 지정할
수 있습니다.

```bash
export JETSON_MQTT_CONFIG=/absolute/path/to/mqtt.yaml
```

설정 구조:

```yaml
broker:
  host: 10.10.20.33
  port: 1883
  qos: 1
```

## 역할별 실행

Repository root에서 각 보드 역할에 맞는 스크립트 하나만 실행합니다.

Jetson A:

```bash
./scripts/run_node_a.sh
```

Jetson B:

```bash
./scripts/run_node_b.sh
```

Jetson D:

```bash
./scripts/run_node_d.sh
```

스크립트는 프로젝트 root로 이동한 다음 다음 Python module을 실행합니다.

```bash
python3 -m src.nodes.node_a
python3 -m src.nodes.node_b
python3 -m src.nodes.node_d
```

현재 하드웨어 기준으로 A는 `/dev/video0`, B는 `/dev/video2`, D는
`http://10.10.20.22:8090/stream`을 사용합니다. 장치 배치가 다르면 실행 전에 각
node의 camera source 설정을 현장 환경에 맞게 확인하십시오.

## MQTT 통신 확인

Broker가 실행 중인 장치 또는 MQTT CLI가 설치된 관리 장치에서 전체 topic을
관찰할 수 있습니다.

```bash
mosquitto_sub -h 10.10.20.33 -p 1883 -q 1 -v -t 'cctv/#'
```

정상 흐름에서는 다음 순서가 관찰됩니다.

1. A가 `cctv/events/a/entry`에 `ENTRY`와 float[512] embedding을 발행
2. Main이 `cctv/candidates/b`에 `WAITING_B_OR_C` 후보를 발행
3. B가 `cctv/events/b/passage`에 `PASSAGE`와 A+B gallery를 발행
4. Main이 `cctv/candidates/d`에 `WAITING_D` 후보를 발행
5. D가 `cctv/events/d/arrival`에 `ARRIVAL`을 발행

## Runtime 파일

실행 중 생성되는 로그, capture, output 및 DB는 Git에 포함되지 않습니다.

```text
logs/
outputs/
captures/
runs/
*.db, *.sqlite, *.sqlite3
```

## Troubleshooting

- `required model files are missing`: `models/MANIFEST.md`의 정확한 경로와
  checksum을 확인합니다.
- `Jetson GPU를 사용할 수 없습니다`: JetPack용 torch/CUDA 설치 상태를 먼저
  확인하고 일반 PyPI torch로 덮어쓰지 않습니다.
- MQTT 연결 거부 또는 timeout: `10.10.20.33:1883` 접근, 방화벽, Broker listener,
  `configs/mqtt.yaml` 또는 `JETSON_MQTT_CONFIG`를 확인합니다.
- 노드가 서로 연결을 끊음: Broker에서 Client ID가 `camera-a`, `camera-b`,
  `camera-d`로 고유한지 확인합니다.
- 카메라를 열 수 없음: `/dev/video*` 권한과 장치 번호, D의 HTTP stream 주소를
  확인합니다.
- `ModuleNotFoundError`: Repository root에서 `scripts/run_node_*.sh`를 사용하고
  현재 활성화된 Jetson Python 환경의 패키지를 확인합니다.
