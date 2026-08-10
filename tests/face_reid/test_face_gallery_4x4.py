from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np


ROOT = Path("/home/aidl/work/pj")

IMAGE_ROOT = ROOT / "outputs" / "face_good_candidates"

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

OUTPUT_DIR = (
    ROOT
    / "outputs"
    / "face_gallery_4x4"
)

FACE_SCORE_THRESHOLD = 0.60


def get_images() -> list[Path]:
    images = []

    for pattern in (
        "*.jpg",
        "*.jpeg",
        "*.png",
    ):
        images.extend(
            IMAGE_ROOT.rglob(pattern)
        )

    return sorted(images)


def detect_best_face(
    detector,
    image: np.ndarray,
):
    # ----------------------------------------
    # 1차: 원본 + threshold 0.60
    # ----------------------------------------
    height, width = image.shape[:2]

    detector.setScoreThreshold(0.60)
    detector.setInputSize((width, height))

    _, faces = detector.detect(image)

    if faces is not None and len(faces) > 0:
        best_face = max(
            faces,
            key=lambda face: float(face[-1]),
        )

        return image, best_face

    # ----------------------------------------
    # 2차: 얼굴이 작으면 2배 확대
    # ----------------------------------------
    enlarged = cv2.resize(
        image,
        None,
        fx=2.0,
        fy=2.0,
        interpolation=cv2.INTER_CUBIC,
    )

    height, width = enlarged.shape[:2]

    detector.setScoreThreshold(0.60)
    detector.setInputSize((width, height))

    _, faces = detector.detect(enlarged)

    if faces is not None and len(faces) > 0:
        best_face = max(
            faces,
            key=lambda face: float(face[-1]),
        )

        print(
            "[FACE] 2배 확대 후 검출 성공 "
            f"CONF={float(best_face[-1]):.3f}"
        )

        return enlarged, best_face

    # ----------------------------------------
    # 3차: 2배 확대 + threshold 0.45
    # ----------------------------------------
    detector.setScoreThreshold(0.45)

    _, faces = detector.detect(enlarged)

    if faces is not None and len(faces) > 0:
        best_face = max(
            faces,
            key=lambda face: float(face[-1]),
        )

        print(
            "[FACE] 2배 확대 + fallback 검출 "
            f"CONF={float(best_face[-1]):.3f}"
        )

        return enlarged, best_face

    raise RuntimeError(
        "원본/2배 확대 모두 얼굴을 검출하지 못했습니다."
    )


def extract_feature(
    image_path: Path,
    detector,
    recognizer,
):
    image = cv2.imread(
        str(image_path)
    )

    if image is None:
        raise RuntimeError(
            f"이미지 읽기 실패: {image_path}"
        )

    detect_image, face = detect_best_face(
        detector,
        image,
    )

    confidence = float(
        face[-1]
    )

    aligned = recognizer.alignCrop(
        detect_image,
        face,
    )

    feature = recognizer.feature(
        aligned
    )

    feature = np.asarray(
        feature,
        dtype=np.float32,
    )

    return (
        feature,
        confidence,
        aligned,
    )

def cosine(
    recognizer,
    feature_a,
    feature_b,
) -> float:
    return float(
        recognizer.match(
            feature_a,
            feature_b,
            cv2.FaceRecognizerSF_FR_COSINE,
        )
    )


def read_four_indices(
    prompt: str,
    max_index: int,
) -> list[int]:
    raw = input(prompt).strip()

    values = [
        int(value)
        for value in raw.split()
    ]

    # 4개 → 2개
    if len(values) != 2:
        raise ValueError(
            "번호를 정확히 2개 입력해야 합니다."
        )

    # 여기도 4 → 2가 되어야 함
    if len(set(values)) != 2:
        raise ValueError(
            "같은 번호를 중복 입력하면 안 됩니다."
        )

    for value in values:
        if value < 0 or value > max_index:
            raise ValueError(
                f"잘못된 번호: {value}"
            )

    return values


def main():
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    images = get_images()

    if len(images) < 8:
        print(
            "A Capture 이미지가 8장 미만입니다."
        )
        return

    print()
    print(
        "========== A CAPTURE IMAGE LIST =========="
    )

    for index, image_path in enumerate(images):
        print(
            f"[{index:02d}] "
            f"{image_path.name}"
        )

    print(
        "=========================================="
    )
    print()

    try:
        group_a_indices = read_four_indices(
            "그룹 A 사진 번호 4개 입력 "
            "(예: 1 2 3 4): ",
            len(images) - 1,
        )

        group_b_indices = read_four_indices(
            "그룹 B 사진 번호 4개 입력 "
            "(예: 10 11 12 13): ",
            len(images) - 1,
        )

    except ValueError as error:
        print()
        print(
            f"[입력 오류] {error}"
        )
        return

    group_a_paths = [
        images[index]
        for index in group_a_indices
    ]

    group_b_paths = [
        images[index]
        for index in group_b_indices
    ]

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

    group_a = []
    group_b = []

    print()
    print(
        "========== FACE FEATURE EXTRACTION =========="
    )

    try:
        for position, path in enumerate(
            group_a_paths,
            start=1,
        ):
            (
                feature,
                confidence,
                aligned,
            ) = extract_feature(
                path,
                detector,
                recognizer,
            )

            group_a.append(
                (
                    path,
                    feature,
                    confidence,
                )
            )

            cv2.imwrite(
                str(
                    OUTPUT_DIR
                    / f"A_{position}.jpg"
                ),
                aligned,
            )

            print(
                f"A{position}: "
                f"{path.name}"
            )
            print(
                f"    Face Confidence = "
                f"{confidence:.4f}"
            )

        for position, path in enumerate(
            group_b_paths,
            start=1,
        ):
            (
                feature,
                confidence,
                aligned,
            ) = extract_feature(
                path,
                detector,
                recognizer,
            )

            group_b.append(
                (
                    path,
                    feature,
                    confidence,
                )
            )

            cv2.imwrite(
                str(
                    OUTPUT_DIR
                    / f"B_{position}.jpg"
                ),
                aligned,
            )

            print(
                f"B{position}: "
                f"{path.name}"
            )
            print(
                f"    Face Confidence = "
                f"{confidence:.4f}"
            )

    except RuntimeError as error:
        print()
        print(
            f"[ERROR] {error}"
        )
        return

    print()
    print(
        "========== 4 x 4 COSINE MATRIX =========="
    )

    scores = []

    print(
        "          B1       B2       B3       B4"
    )

    for a_index, (_, feature_a, _) in enumerate(
        group_a,
        start=1,
    ):
        row_scores = []

        for _, feature_b, _ in group_b:
            score = cosine(
                recognizer,
                feature_a,
                feature_b,
            )

            row_scores.append(
                score
            )

            scores.append(
                score
            )

        print(
            f"A{a_index}   "
            + "  ".join(
                f"{score:.4f}"
                for score in row_scores
            )
        )

    scores_np = np.asarray(
        scores,
        dtype=np.float32,
    )

    sorted_scores = np.sort(
        scores_np
    )[::-1]

    top3 = sorted_scores[:3]

    print()
    print(
        "============== SUMMARY =============="
    )

    print(
        f"비교 횟수        : "
        f"{len(scores_np)}"
    )

    print(
        f"최고 점수 BEST   : "
        f"{scores_np.max():.6f}"
    )

    print(
        f"최저 점수 MIN    : "
        f"{scores_np.min():.6f}"
    )

    print(
        f"전체 평균 MEAN   : "
        f"{scores_np.mean():.6f}"
    )

    print(
        f"중앙값 MEDIAN    : "
        f"{np.median(scores_np):.6f}"
    )

    print(
        f"TOP3 평균        : "
        f"{top3.mean():.6f}"
    )

    print(
        f"표준편차 STD     : "
        f"{scores_np.std():.6f}"
    )

    print(
        "====================================="
    )

    print()
    print(
        f"정렬된 얼굴 8장 저장:"
    )
    print(
        OUTPUT_DIR
    )


if __name__ == "__main__":
    main()