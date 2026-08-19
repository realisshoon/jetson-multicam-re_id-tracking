# Camera D

- 역할: 최종 도착 인물 검출/추적, OSNet Re-ID 도착 검증, Stranger 감지
- 실행 파일: `src/nodes/node_d.py`
- 실행 스크립트: `scripts/run_node_d.sh`
- requirements: `requirements/camera-d.txt`
- 기본 스트림 포트: `8003`

## 필요한 모델

- `yolo26n.pt`
- `models/reid/person_reid_osnet_x0_25_fp16.engine`

## 실행

```bash
python3 -m venv .venv --system-site-packages
source .venv/bin/activate
pip install -r requirements/camera-d.txt
./scripts/run_node_d.sh
```

MQTT Broker 주소와 모델 파일 배치 후 실행합니다. 전체 기동 순서는 `실행방법.md`를 참고합니다.
