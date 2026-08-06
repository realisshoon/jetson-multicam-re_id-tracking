# Jetson Re-ID Deployment Package

## 모델

- Model: OSNet x0.25
- Format: ONNX
- Input: float32 NCHW [batch, 3, 256, 128]
- Output: float32 [batch, 512]
- Output embedding은 모델 내부에서 L2 정규화됨

## 전처리

1. ByteTrack bbox로 사람 crop 추출
2. OpenCV BGR → RGB
3. 128 x 256으로 resize
4. 0~1 범위로 scale
5. ImageNet mean/std 정규화
6. HWC → CHW
7. batch 차원 추가

## ByteTrack에서 전달할 데이터

- camera_id
- local_track_id
- timestamp
- bbox_xyxy
- person_crop
- track event

## Re-ID 출력

- 512차원 L2-normalized embedding

## 주의

이 패키지의 유사도 점수는 ONNX 변환 검증용이다.
최종 동일인 임계값은 C270 자체 데이터로 다시 산출한다.

TensorRT Engine 파일은 Jetson 장치에서 직접 생성한다.
