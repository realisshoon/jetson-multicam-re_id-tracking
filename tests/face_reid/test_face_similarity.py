from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np


ROOT = Path("/home/aidl/work/pj")

IMAGE_ROOT = ROOT / "outputs" / "captures" / "A"

YUNET_MODEL = (
    ROOT
    / "models"
    / "face"
    / "face_detection_yunet_2023mar.onnx"
)

SFACE_MODEL = (
    ROOT
    / "models"
    / "face"
    / "face_recognition_sface_2021dec.onnx"
)

FACE_SCORE_THRESHOLD = 0.60


def detect_best_face(detector, image: np.ndarray):
    height, width = image.shape[:2]

    detector.setInputSize((width, height))

    _, faces = detector.detect(image)

    if faces is None or len(faces) == 0:
        raise RuntimeError("얼굴을 검출하지 못했습니다.")

    return max(
        faces,
        key=lambda face: float(face[-1]),
    )


def extract_face_feature(
    image_path: Path,
    detector,
    recognizer,
):
    image = cv2.imread(str(image_path))

    if image is None:
        raise RuntimeError(
            f"이미지를 읽을 수 없습니다: {image_path}"
        )

    face = detect_best_face(
        detector,
        image,
    )

    confidence = float(face[-1])

    aligned = recognizer.alignCrop(
        image,
        face,
    )

    feature = recognizer.feature(
        aligned
    )

    feature = np.asarray(
        feature,
        dtype=np.float32,
    )

    return feature, confidence, aligned


def get_image_list() -> list[Path]:
    images = []

    for extension in (
        "*.jpg",
        "*.jpeg",
        "*.png",
    ):
        images.extend(
            IMAGE_ROOT.rglob(extension)
        )

    return sorted(images)


def main():
    images = get_image_list()

    if len(images) < 2:
        print(
            f"비교할 이미지가 부족합니다: {IMAGE_ROOT}"
        )
        return

    print()
    print("========== A CAPTURE IMAGE LIST ==========")

    for index, image_path in enumerate(images):
        print(
            f"[{index:02d}] "
            f"{image_path.name}"
        )

    print("==========================================")
    print()

    try:
        first_index = int(
            input("첫 번째 사진 번호 입력: ")
        )

        second_index = int(
            input("두 번째 사진 번호 입력: ")
        )

        image1_path = images[first_index]
        image2_path = images[second_index]

    except (ValueError, IndexError):
        print("잘못된 번호입니다.")
        return

    print()
    print("선택한 이미지")
    print(f"1번: {image1_path}")
    print(f"2번: {image2_path}")

    detector = cv2.FaceDetectorYN.create(
        str(YUNET_MODEL),
        "",
        (320, 320),
        FACE_SCORE_THRESHOLD,
        0.3,
        5000,
    )

    recognizer = cv2.FaceRecognizerSF.create(
        str(SFACE_MODEL),
        "",
    )

    try:
        (
            feature1,
            confidence1,
            aligned1,
        ) = extract_face_feature(
            image1_path,
            detector,
            recognizer,
        )

        (
            feature2,
            confidence2,
            aligned2,
        ) = extract_face_feature(
            image2_path,
            detector,
            recognizer,
        )

    except RuntimeError as error:
        print()
        print(f"[ERROR] {error}")
        return

    cosine_score = recognizer.match(
        feature1,
        feature2,
        cv2.FaceRecognizerSF_FR_COSINE,
    )

    l2_score = recognizer.match(
        feature1,
        feature2,
        cv2.FaceRecognizerSF_FR_NORM_L2,
    )

    print()
    print("===== SFACE SIMILARITY TEST =====")
    print(f"Image 1 : {image1_path.name}")
    print(
        f"Face Confidence 1 : "
        f"{confidence1:.4f}"
    )

    print()

    print(f"Image 2 : {image2_path.name}")
    print(
        f"Face Confidence 2 : "
        f"{confidence2:.4f}"
    )

    print()

    print(
        f"Cosine Similarity : "
        f"{cosine_score:.6f}"
    )

    print(
        f"L2 Distance       : "
        f"{l2_score:.6f}"
    )

    print("=================================")

    output_dir = (
        ROOT
        / "outputs"
        / "face_similarity_test"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    cv2.imwrite(
        str(output_dir / "face_1_aligned.jpg"),
        aligned1,
    )

    cv2.imwrite(
        str(output_dir / "face_2_aligned.jpg"),
        aligned2,
    )

    print()
    print(
        f"정렬 얼굴 저장: {output_dir}"
    )


if __name__ == "__main__":
    main()