import cv2
import numpy as np


INPUT_WIDTH = 128
INPUT_HEIGHT = 256

MEAN = np.array(
    [0.485, 0.456, 0.406],
    dtype=np.float32,
)

STD = np.array(
    [0.229, 0.224, 0.225],
    dtype=np.float32,
)


def preprocess_bgr(person_crop):
    """
    ByteTrack bbox로 자른 OpenCV BGR 이미지를
    Re-ID 모델 입력으로 변환한다.

    입력:
        uint8 BGR [H, W, 3]

    출력:
        float32 NCHW [1, 3, 256, 128]
    """

    if person_crop is None:
        raise ValueError("person_crop is None")

    if person_crop.size == 0:
        raise ValueError("person_crop is empty")

    rgb = cv2.cvtColor(
        person_crop,
        cv2.COLOR_BGR2RGB,
    )

    resized = cv2.resize(
        rgb,
        (INPUT_WIDTH, INPUT_HEIGHT),
        interpolation=cv2.INTER_LINEAR,
    )

    tensor = resized.astype(
        np.float32
    )

    tensor /= 255.0
    tensor = (tensor - MEAN) / STD

    tensor = np.transpose(
        tensor,
        (2, 0, 1),
    )

    tensor = np.expand_dims(
        tensor,
        axis=0,
    )

    return np.ascontiguousarray(
        tensor,
        dtype=np.float32,
    )


def cosine_similarity(embedding_a, embedding_b):
    """
    모델 출력은 이미 L2 정규화된 상태이므로
    dot product가 cosine similarity가 된다.
    """

    a = np.asarray(
        embedding_a,
        dtype=np.float32,
    ).reshape(-1)

    b = np.asarray(
        embedding_b,
        dtype=np.float32,
    ).reshape(-1)

    return float(np.dot(a, b))
