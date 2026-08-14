from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

import numpy as np


EMBEDDING_DIM = 512
SCHEMA_VERSION = 1


def current_timestamp() -> str:
    """현재 시각을 로컬 타임존 ISO 형식으로 반환한다."""
    return datetime.now().astimezone().isoformat(
        timespec="seconds"
    )


def embedding_to_list(
    embedding: np.ndarray | list[float],
) -> list[float]:
    """
    Re-ID embedding을 검증하고 L2 정규화한 뒤
    MQTT JSON 전송 가능한 list 형태로 반환한다.
    """

    array = np.asarray(
        embedding,
        dtype=np.float32,
    ).reshape(-1)

    if array.size != EMBEDDING_DIM:
        raise ValueError(
            f"Embedding 크기가 잘못됐습니다: "
            f"{array.size}, 예상값: {EMBEDDING_DIM}"
        )

    if not np.all(np.isfinite(array)):
        raise ValueError(
            "Embedding에 NaN 또는 Inf가 포함됐습니다."
        )

    norm = float(np.linalg.norm(array))

    if norm <= 1e-12:
        raise ValueError(
            "Embedding Norm이 0입니다."
        )

    normalized = array / norm

    return normalized.astype(
        np.float32
    ).tolist()


def make_gallery_entry(
    node_id: str,
    embedding: np.ndarray | list[float],
    captured_at: str | None = None,
    quality: float | None = None,
) -> dict[str, Any]:
    """
    A 또는 B에서 얻은 특징값 하나를 Gallery 항목으로 만든다.
    """

    entry: dict[str, Any] = {
        "node_id": node_id,
        "captured_at": (
            captured_at
            if captured_at is not None
            else current_timestamp()
        ),
        "embedding_dim": EMBEDDING_DIM,
        "embedding": embedding_to_list(
            embedding
        ),
    }

    if quality is not None:
        entry["quality"] = float(
            max(0.0, min(1.0, quality))
        )

    return entry


def build_passage_payload(
    journey_id: str,
    entry_timestamp: str,
    a_embedding: np.ndarray | list[float],
    b_embeddings: Iterable[
        np.ndarray | list[float]
    ],
    a_local_track_id: int | str,
    b_local_track_id: int,
    b_passage_timestamp: str | None = None,
) -> dict[str, Any]:
    """
    B에서 D로 전송할 A+B 이동 정보를 생성한다.
    """

    passage_timestamp = (
        b_passage_timestamp
        if b_passage_timestamp is not None
        else current_timestamp()
    )

    gallery: list[dict[str, Any]] = [
        make_gallery_entry(
            node_id="A",
            embedding=a_embedding,
            captured_at=entry_timestamp,
        )
    ]

    for embedding in b_embeddings:
        gallery.append(
            make_gallery_entry(
                node_id="B",
                embedding=embedding,
                captured_at=passage_timestamp,
            )
        )

    if len(gallery) < 2:
        raise ValueError(
            "B 특징값이 하나 이상 필요합니다."
        )

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event": "PASSAGE",
        "journey_id": journey_id,

        # 현재 코드와의 호환성 유지
        "global_person_id": journey_id,

        "current_node": "B",
        "route": ["A", "B"],
        "next_nodes": ["D"],

        "entry_timestamp": entry_timestamp,
        "b_passage_timestamp": passage_timestamp,

        "a_local_track_id": a_local_track_id,
        "b_local_track_id": b_local_track_id,

        "gallery_count": len(gallery),
        "gallery": gallery,
    }

    return payload