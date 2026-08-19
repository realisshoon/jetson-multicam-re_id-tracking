# Camera A

- 역할: 입장 인물 검출/추적, Body 512-D Re-ID 임베딩, YuNet/SFace 얼굴 특징 추출, ENTRY 발행
- 실행 파일: `src/nodes/node_a.py`
- 실행 스크립트: `scripts/run_node_a.sh`
- requirements: `requirements/camera-a.txt`
- 기본 스트림 포트: `8000`

## 필요한 모델

- `yolo26n.pt`
- `models/reid/person_reid_osnet_x0_25_fp16.engine`
- `models/face/face_detection_yunet_2023mar.onnx`
- `models/face/face_recognition_sface_2021dec.onnx`

## 실행

```bash
python3 -m venv .venv --system-site-packages
source .venv/bin/activate
pip install -r requirements/camera-a.txt
./scripts/run_node_a.sh
```

MQTT Broker 주소와 모델 파일 배치 후 실행합니다. 전체 기동 순서는 `실행방법.md`를 참고합니다.
