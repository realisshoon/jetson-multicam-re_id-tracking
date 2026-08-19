# 최종 제출 체크리스트

교육장 안내 화면의 최종 제출 형식에 맞춘 체크리스트입니다.

## 최종 압축 폴더명

```text
최종_결과물_팀번호_조원이름
```

예시:
```text
최종_결과물_01_조성재김동현박경민
```

## 압축 폴더에 포함할 파일

```text
최종_결과물_팀번호_조원이름/
├─ 최종_결과보고서_팀번호.pptx
├─ 최종_결과보고서_팀번호.pdf
├─ 시연영상_팀번호.mp4
├─ requirements.txt
├─ 실행방법.md
└─ jetson-multicam-re_id-tracking/
   ├─ src/
   ├─ cctv_main/
   ├─ web/
   ├─ configs/
   ├─ scripts/
   ├─ requirements/
   ├─ models/
   ├─ board_projects/
   ├─ README.md
   └─ ...
```

## 현재 Git 저장소에 준비된 항목

- [x] 프로젝트 전체 소스코드
- [x] 루트 `requirements.txt`
- [x] 보드/역할별 requirements (`requirements/`)
- [x] 실행 스크립트 (`scripts/run_node_a.sh` ~ `run_node_d.sh`)
- [x] Main Server / Web 소스
- [x] 환경 및 MQTT/포트 문서
- [x] 모델 배치 경로와 체크섬 (`models/MANIFEST.md`)
- [x] 최종 제출용 `실행방법.md`
- [x] Jetson 보드별 프로젝트 안내 (`board_projects/`)

## 제출 직전에 별도로 추가해야 하는 항목

아래 파일은 Git 저장소에 자동 포함되지 않으므로 **최종 ZIP 생성 전에 직접 추가**합니다.

- [ ] `최종_결과보고서_팀번호.pptx`
- [ ] `최종_결과보고서_팀번호.pdf`
- [ ] PPT에서 사용한 폰트가 별도 제출 대상이면 폰트 파일 포함
- [ ] `시연영상_팀번호.mp4`
- [ ] 실제 실행용 AI 모델 파일
  - [ ] `yolo26n.pt`
  - [ ] `models/reid/person_reid_osnet_x0_25_fp16.engine`
  - [ ] `models/face/face_detection_yunet_2023mar.onnx`
  - [ ] `models/face/face_recognition_sface_2021dec.onnx`

> TensorRT `.engine` 파일은 Jetson 환경에 종속될 수 있으므로 실제 시연에 사용한 검증된 파일을 제출합니다.

## 보드별 프로젝트 구분

Jetson을 여러 대 사용하므로 `board_projects/`에서 각 보드 역할을 분리해 두었습니다.

- `board_projects/Camera_A/` : 입장 / Body + Face 특징 추출
- `board_projects/Camera_B/` : B 경유 Re-ID
- `board_projects/Camera_C/` : C 경유 Re-ID
- `board_projects/Camera_D/` : 도착 검증 / Stranger 감지

실제 공통 구현은 루트의 `src/`, `configs/`, `models/`를 공유합니다. 각 보드 폴더의 README에서 해당 보드의 실행 파일, requirements, 필요한 모델을 바로 확인할 수 있습니다.

## 최종 제출 전 확인

- [ ] 제출용 ZIP 압축 해제 후 폴더 구조가 정상인지 확인
- [ ] `실행방법.md`가 열리는지 확인
- [ ] PPT와 PDF가 모두 열리는지 확인
- [ ] 시연영상 재생 확인
- [ ] 모델 파일 누락 여부 확인
- [ ] `.env`, 토큰, 비밀번호 등 개인 인증정보가 포함되지 않았는지 확인
- [ ] `data/`, 로그, DB 백업, `__pycache__`, 가상환경 폴더 등 불필요한 파일 제거
- [ ] 최종 시연에 사용한 코드와 `submission/final` 브랜치가 일치하는지 확인
