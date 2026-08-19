# Camera C

- 역할: C 경유 구간 인물 검출/추적, OSNet Re-ID 비교, PASSAGE 발행
- 실행 파일: `src/nodes/node_c.py`
- 실행 스크립트: `scripts/run_node_c.sh`
- requirements: `requirements/camera-c.txt`
- 기본 스트림 포트: `8002`

## 필요한 모델

- `yolo26n.pt`
- `models/reid/person_reid_osnet_x0_25_fp16.engine`

## 실행

```bash
python3 -m venv .venv --system-site-packages
source .venv/bin/activate
pip install -r requirements/camera-c.txt
./scripts/run_node_c.sh
```

MQTT Broker 주소와 모델 파일 배치 후 실행합니다. 전체 기동 순서는 `실행방법.md`를 참고합니다.
