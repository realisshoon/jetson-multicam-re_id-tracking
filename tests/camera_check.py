from pathlib import Path
import glob

import cv2


# 테스트 이미지를 저장할 위치
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "outputs" / "images" / "c270_test.jpg"


def open_camera(device_path: str):
    """
    /dev/videoN 장치를 열고 실제 영상 프레임이 나오는지 검사한다.
    """
    print(f"\n[테스트 중] {device_path}")

    # Jetson/Linux V4L2 방식으로 카메라 열기
    cap = cv2.VideoCapture(device_path, cv2.CAP_V4L2)

    if not cap.isOpened():
        print("카메라 열기 실패")
        return None

    # C270에서 720p 전송 부담을 줄이기 위해 MJPEG 사용
    cap.set(
        cv2.CAP_PROP_FOURCC,
        cv2.VideoWriter_fourcc(*"MJPG")
    )
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
    cap.set(cv2.CAP_PROP_FPS, 30)

    frame = None
    success_count = 0

    # 자동 노출이 안정화되도록 여러 프레임 읽기
    for _ in range(30):
        success, current_frame = cap.read()

        if success and current_frame is not None:
            frame = current_frame
            success_count += 1

    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS)

    cap.release()

    if frame is None:
        print("프레임 읽기 실패")
        return None

    print("카메라 프레임 읽기 성공")
    print(f"성공 프레임 수: {success_count}/30")
    print(f"해상도: {width} x {height}")
    print(f"카메라 FPS 설정값: {fps:.2f}")

    return frame


def main():
    # /dev/video0, /dev/video1 등을 자동으로 탐색
    devices = sorted(glob.glob("/dev/video*"))

    if not devices:
        print("오류: /dev/video 장치를 찾지 못했습니다.")
        print("C270 USB 연결 상태를 확인하세요.")
        return

    print("발견된 영상 장치:")

    for device in devices:
        print(f"  {device}")

    # 실제 프레임이 나오는 첫 번째 장치를 사용
    for device in devices:
        frame = open_camera(device)

        if frame is None:
            continue

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        saved = cv2.imwrite(str(OUTPUT_PATH), frame)

        if not saved:
            print("이미지 저장 실패")
            return

        print("\n===== C270 테스트 성공 =====")
        print(f"사용 가능한 장치: {device}")
        print(f"저장된 이미지: {OUTPUT_PATH}")
        return

    print("\n오류: 영상 프레임을 읽을 수 있는 장치가 없습니다.")


if __name__ == "__main__":
    main()