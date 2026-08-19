# 보드별 프로젝트 구성

최종 제출 안내의 "보드 여러 대 사용 시 보드별 프로젝트 폴더 생성" 항목에 맞춰 Jetson 4대의 역할을 구분한 안내 폴더입니다.

공통 소스와 모델/설정은 저장소 루트의 `src/`, `models/`, `configs/`, `scripts/`를 공유하며, 각 보드 폴더에서 해당 보드가 사용하는 실행 파일과 requirements를 확인할 수 있습니다.

| 보드 | 역할 | 실행 파일 | requirements |
|---|---|---|---|
| Camera A | 입장 / Body + Face 특징 추출 | `src/nodes/node_a.py` | `requirements/camera-a.txt` |
| Camera B | B 경유 Re-ID | `src/nodes/node_b.py` | `requirements/camera-b.txt` |
| Camera C | C 경유 Re-ID | `src/nodes/node_c.py` | `requirements/camera-c.txt` |
| Camera D | 도착 검증 / Stranger 감지 | `src/nodes/node_d.py` | `requirements/camera-d.txt` |

실행 순서는 루트의 `실행방법.md`를 참고합니다.
