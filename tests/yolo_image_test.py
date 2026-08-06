from pathlib import Path

import cv2
import torch
from ultralytics import YOLO


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_IMAGE = (
    PROJECT_ROOT / "outputs" / "images" / "c270_test.jpg"
)

OUTPUT_IMAGE = (
    PROJECT_ROOT / "outputs" / "images" / "yolo_result.jpg"
)


def main():
    if not INPUT_IMAGE.exists():
        raise FileNotFoundError(f"입력 이미지 없음: {INPUT_IMAGE}")

    print("CUDA 사용 가능:", torch.cuda.is_available())
    print("사용 GPU:", torch.cuda.get_device_name(0))

    # 최초 실행 시 사전 학습 모델이 자동 다운로드됨
    model = YOLO("yolo26n.pt")

    results = model.predict(
        source=str(INPUT_IMAGE),
        device=0,
        classes=[0],       # COCO class 0 = person
        conf=0.25,
        verbose=True,
    )

    result = results[0]
    person_count = len(result.boxes)

    annotated_image = result.plot()

    OUTPUT_IMAGE.parent.mkdir(parents=True, exist_ok=True)

    if not cv2.imwrite(str(OUTPUT_IMAGE), annotated_image):
        raise RuntimeError("결과 이미지 저장 실패")

    print("검출된 사람 수:", person_count)
    print("결과 이미지:", OUTPUT_IMAGE)


if __name__ == "__main__":
    main()