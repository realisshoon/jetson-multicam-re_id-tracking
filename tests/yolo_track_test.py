from pathlib import Path
import time

import cv2
import torch
from ultralytics import YOLO


PROJECT_ROOT = Path(__file__).resolve().parents[1]

MODEL_PATH = PROJECT_ROOT / "yolo26n.pt"
OUTPUT_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "videos"
    / "bytetrack_test.mp4"
)

CAMERA_DEVICE = 0
WIDTH = 1280
HEIGHT = 720
FPS = 30
TEST_SECONDS = 15


def main():
    print("CUDA 사용 가능:", torch.cuda.is_available())

    if not torch.cuda.is_available():
        raise RuntimeError("Jetson GPU를 사용할 수 없습니다.")

    model = YOLO(str(MODEL_PATH))

    cap = cv2.VideoCapture(CAMERA_DEVICE, cv2.CAP_V4L2)

    if not cap.isOpened():
        raise RuntimeError("/dev/video0 카메라를 열 수 없습니다.")

    cap.set(
        cv2.CAP_PROP_FOURCC,
        cv2.VideoWriter_fourcc(*"MJPG"),
    )
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, HEIGHT)
    cap.set(cv2.CAP_PROP_FPS, FPS)

    actual_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    actual_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    writer = cv2.VideoWriter(
        str(OUTPUT_PATH),
        cv2.VideoWriter_fourcc(*"mp4v"),
        FPS,
        (actual_width, actual_height),
    )

    if not writer.isOpened():
        cap.release()
        raise RuntimeError("결과 영상 파일을 만들 수 없습니다.")

    print(f"카메라: /dev/video{CAMERA_DEVICE}")
    print(f"해상도: {actual_width} x {actual_height}")
    print(f"테스트 시간: {TEST_SECONDS}초")

    start_time = time.time()
    frame_count = 0

    try:
        while time.time() - start_time < TEST_SECONDS:
            success, frame = cap.read()

            if not success:
                print("프레임 읽기 실패")
                continue

            results = model.track(
                source=frame,
                persist=True,
                tracker="bytetrack.yaml",
                classes=[0],       # person만 검출
                conf=0.25,
                device=0,
                verbose=False,
            )

            result = results[0]
            annotated_frame = result.plot()

            track_ids = []

            if (
                result.boxes is not None
                and result.boxes.id is not None
            ):
                track_ids = (
                    result.boxes.id
                    .int()
                    .cpu()
                    .tolist()
                )

            print(
                f"\r프레임: {frame_count:4d} | "
                f"현재 사람 ID: {track_ids}",
                end="",
                flush=True,
            )

            writer.write(annotated_frame)
            frame_count += 1

    finally:
        cap.release()
        writer.release()

    print()
    print("===== ByteTrack 테스트 완료 =====")
    print("처리 프레임:", frame_count)
    print("저장 영상:", OUTPUT_PATH)


if __name__ == "__main__":
    main()