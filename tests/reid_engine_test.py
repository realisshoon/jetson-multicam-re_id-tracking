from __future__ import annotations

import sys
from pathlib import Path

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(
        0,
        str(PROJECT_ROOT),
    )


from src.reid.preprocess import cosine_similarity
from src.reid.reid_engine import ReIDTensorRTEngine


ENGINE_PATH = (
    PROJECT_ROOT
    / "models"
    / "reid"
    / "person_reid_osnet_x0_25_fp16.engine"
)

SAMPLE_DIR = (
    PROJECT_ROOT
    / "external"
    / "reid_jetson_package"
    / "samples"
)


def print_embedding_info(
    name: str,
    embedding: np.ndarray,
) -> None:
    norm = float(
        np.linalg.norm(embedding)
    )

    print(
        f"{name:12s} "
        f"shape={embedding.shape}, "
        f"norm={norm:.6f}"
    )


def main() -> None:
    engine = ReIDTensorRTEngine(
        ENGINE_PATH
    )

    image_paths = {
        "positive_a": (
            SAMPLE_DIR / "positive_a.jpg"
        ),
        "positive_b": (
            SAMPLE_DIR / "positive_b.jpg"
        ),
        "negative_a": (
            SAMPLE_DIR / "negative_a.jpg"
        ),
        "negative_b": (
            SAMPLE_DIR / "negative_b.jpg"
        ),
    }

    embeddings: dict[str, np.ndarray] = {}

    print()
    print("===== Embedding 추출 =====")

    for name, image_path in image_paths.items():
        embedding = engine.extract_from_file(
            image_path
        )

        embeddings[name] = embedding

        print_embedding_info(
            name,
            embedding,
        )

    positive_score = cosine_similarity(
        embeddings["positive_a"],
        embeddings["positive_b"],
    )

    negative_score = cosine_similarity(
        embeddings["negative_a"],
        embeddings["negative_b"],
    )

    print()
    print("===== Re-ID 유사도 결과 =====")
    print(
        f"동일인 Positive 점수: "
        f"{positive_score:.6f}"
    )
    print(
        f"타인 Negative 점수 : "
        f"{negative_score:.6f}"
    )
    print(
        f"점수 차이           : "
        f"{positive_score - negative_score:.6f}"
    )
    print("=============================")

    embedding_size_ok = all(
        embedding.size == 512
        for embedding in embeddings.values()
    )

    norm_ok = all(
        0.95
        <= float(np.linalg.norm(embedding))
        <= 1.05
        for embedding in embeddings.values()
    )

    similarity_ok = (
        positive_score > negative_score
    )

    if (
        embedding_size_ok
        and norm_ok
        and similarity_ok
    ):
        print()
        print(
            "RESULT: PASS - "
            "Re-ID TensorRT 엔진 정상"
        )

    else:
        print()
        print(
            "RESULT: FAIL - "
            "출력 값을 확인하세요."
        )

        print(
            f"512차원 여부: "
            f"{embedding_size_ok}"
        )

        print(
            f"L2 Norm 여부: "
            f"{norm_ok}"
        )

        print(
            f"동일인 점수 우세: "
            f"{similarity_ok}"
        )


if __name__ == "__main__":
    main()