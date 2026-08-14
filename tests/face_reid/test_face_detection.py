from pathlib import Path

import cv2


ROOT = Path("/home/aidl/work/pj")

IMAGE_ROOT = ROOT / "outputs" / "captures" / "A"

YUNET_MODEL = (
    ROOT
    / "models"
    / "face"
    / "face_detection_yunet_2023mar.onnx"
)

OUTPUT_DIR = (
    ROOT
    / "outputs"
    / "face_detection_test"
)

SCORE_THRESHOLD = 0.60


def main():
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    detector = cv2.FaceDetectorYN.create(
        str(YUNET_MODEL),
        "",
        (320, 320),
        SCORE_THRESHOLD,
        0.3,
        5000,
    )

    image_paths = []

    for extension in (
        "*.jpg",
        "*.jpeg",
        "*.png",
    ):
        image_paths.extend(
            IMAGE_ROOT.rglob(extension)
        )

    image_paths = sorted(image_paths)

    if not image_paths:
        print(
            f"A Capture 이미지가 없습니다: "
            f"{IMAGE_ROOT}"
        )
        return

    total_count = 0
    detected_count = 0
    total_faces = 0

    print()
    print("===== A CAPTURE FACE DETECTION TEST =====")
    print(f"Input : {IMAGE_ROOT}")
    print(f"Model : {YUNET_MODEL}")
    print(f"Score : {SCORE_THRESHOLD}")
    print("=========================================")
    print()

    for image_path in image_paths:
        frame = cv2.imread(
            str(image_path)
        )

        if frame is None:
            print(
                f"[READ FAIL] "
                f"{image_path.name}"
            )
            continue

        total_count += 1

        height, width = frame.shape[:2]

        detector.setInputSize(
            (width, height)
        )

        _, faces = detector.detect(
            frame
        )

        annotated = frame.copy()

        face_count = (
            0
            if faces is None
            else len(faces)
        )

        if face_count > 0:
            detected_count += 1
            total_faces += face_count

        print(
            f"[{total_count:03d}] "
            f"{image_path.name} "
            f"→ FACE {face_count}"
        )

        if faces is not None:
            for face in faces:
                x = int(face[0])
                y = int(face[1])
                w = int(face[2])
                h = int(face[3])
                score = float(face[-1])

                x1 = max(0, x)
                y1 = max(0, y)
                x2 = min(
                    width - 1,
                    x + w,
                )
                y2 = min(
                    height - 1,
                    y + h,
                )

                cv2.rectangle(
                    annotated,
                    (x1, y1),
                    (x2, y2),
                    (0, 255, 0),
                    2,
                )

                cv2.putText(
                    annotated,
                    f"FACE {score:.3f}",
                    (
                        x1,
                        max(20, y1 - 8),
                    ),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.55,
                    (0, 255, 0),
                    2,
                )

        output_path = (
            OUTPUT_DIR
            / image_path.name
        )

        cv2.imwrite(
            str(output_path),
            annotated,
        )

    detection_rate = (
        detected_count
        / total_count
        * 100.0
        if total_count
        else 0.0
    )

    print()
    print("========== RESULT ==========")
    print(
        f"전체 이미지 : "
        f"{total_count}"
    )
    print(
        f"얼굴 검출   : "
        f"{detected_count}"
    )
    print(
        f"미검출      : "
        f"{total_count - detected_count}"
    )
    print(
        f"총 얼굴 수  : "
        f"{total_faces}"
    )
    print(
        f"검출률      : "
        f"{detection_rate:.1f}%"
    )
    print(
        f"결과 이미지 : "
        f"{OUTPUT_DIR}"
    )
    print("============================")


if __name__ == "__main__":
    main()