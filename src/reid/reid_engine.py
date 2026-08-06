from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np
import tensorrt as trt
import torch

from src.reid.preprocess import preprocess_bgr


TRT_LOGGER = trt.Logger(trt.Logger.WARNING)


def trt_dtype_to_torch(
    dtype: trt.DataType,
) -> torch.dtype:
    """TensorRT 데이터 타입을 PyTorch 데이터 타입으로 변환한다."""

    dtype_map = {
        trt.float32: torch.float32,
        trt.float16: torch.float16,
        trt.int8: torch.int8,
        trt.int32: torch.int32,
        trt.bool: torch.bool,
    }

    if dtype not in dtype_map:
        raise TypeError(
            f"지원하지 않는 TensorRT 데이터 타입: {dtype}"
        )

    return dtype_map[dtype]


class ReIDTensorRTEngine:
    """
    OSNet TensorRT 엔진으로 사람 이미지의
    512차원 Re-ID embedding을 추출한다.
    """

    def __init__(
        self,
        engine_path: str | Path,
    ) -> None:
        self.engine_path = Path(engine_path)

        if not self.engine_path.exists():
            raise FileNotFoundError(
                f"TensorRT 엔진이 없습니다: {self.engine_path}"
            )

        if not torch.cuda.is_available():
            raise RuntimeError(
                "CUDA GPU를 사용할 수 없습니다."
            )

        engine_data = self.engine_path.read_bytes()

        self.runtime = trt.Runtime(TRT_LOGGER)

        self.engine = self.runtime.deserialize_cuda_engine(
            engine_data
        )

        if self.engine is None:
            raise RuntimeError(
                "TensorRT 엔진 역직렬화에 실패했습니다."
            )

        self.context = self.engine.create_execution_context()

        if self.context is None:
            raise RuntimeError(
                "TensorRT 실행 Context 생성에 실패했습니다."
            )

        self.input_names: list[str] = []
        self.output_names: list[str] = []

        for index in range(
            self.engine.num_io_tensors
        ):
            tensor_name = self.engine.get_tensor_name(
                index
            )

            tensor_mode = self.engine.get_tensor_mode(
                tensor_name
            )

            if tensor_mode == trt.TensorIOMode.INPUT:
                self.input_names.append(tensor_name)

            elif tensor_mode == trt.TensorIOMode.OUTPUT:
                self.output_names.append(tensor_name)

        if len(self.input_names) != 1:
            raise RuntimeError(
                f"입력 텐서가 1개가 아닙니다: "
                f"{self.input_names}"
            )

        if len(self.output_names) != 1:
            raise RuntimeError(
                f"출력 텐서가 1개가 아닙니다: "
                f"{self.output_names}"
            )

        self.input_name = self.input_names[0]
        self.output_name = self.output_names[0]

        print("===== Re-ID TensorRT Engine =====")
        print(f"Engine : {self.engine_path}")
        print(f"Input  : {self.input_name}")
        print(
            f"Shape  : "
            f"{tuple(self.engine.get_tensor_shape(self.input_name))}"
        )
        print(f"Output : {self.output_name}")
        print(
            f"Shape  : "
            f"{tuple(self.engine.get_tensor_shape(self.output_name))}"
        )
        print("=================================")

    def extract(
        self,
        person_crop: np.ndarray,
    ) -> np.ndarray:
        """
        OpenCV BGR 사람 crop에서
        512차원 embedding을 추출한다.

        반환:
            float32 [512]
        """

        if person_crop is None:
            raise ValueError(
                "person_crop이 None입니다."
            )

        if person_crop.size == 0:
            raise ValueError(
                "person_crop이 비어 있습니다."
            )

        # BGR 이미지 → float32 NCHW [1, 3, 256, 128]
        input_array = preprocess_bgr(
            person_crop
        )

        input_dtype = trt_dtype_to_torch(
            self.engine.get_tensor_dtype(
                self.input_name
            )
        )

        input_tensor = (
            torch.from_numpy(input_array)
            .to(
                device="cuda",
                dtype=input_dtype,
            )
            .contiguous()
        )

        input_shape = tuple(
            input_tensor.shape
        )

        shape_success = self.context.set_input_shape(
            self.input_name,
            input_shape,
        )

        if not shape_success:
            raise RuntimeError(
                f"입력 Shape 설정 실패: {input_shape}"
            )

        output_shape = tuple(
            int(value)
            for value in self.context.get_tensor_shape(
                self.output_name
            )
        )

        if any(
            value < 0
            for value in output_shape
        ):
            raise RuntimeError(
                f"출력 Shape가 확정되지 않았습니다: "
                f"{output_shape}"
            )

        output_dtype = trt_dtype_to_torch(
            self.engine.get_tensor_dtype(
                self.output_name
            )
        )

        output_tensor = torch.empty(
            output_shape,
            device="cuda",
            dtype=output_dtype,
        )

        input_address_success = (
            self.context.set_tensor_address(
                self.input_name,
                int(input_tensor.data_ptr()),
            )
        )

        output_address_success = (
            self.context.set_tensor_address(
                self.output_name,
                int(output_tensor.data_ptr()),
            )
        )

        if not input_address_success:
            raise RuntimeError(
                "입력 텐서 주소 설정 실패"
            )

        if not output_address_success:
            raise RuntimeError(
                "출력 텐서 주소 설정 실패"
            )

        stream = torch.cuda.current_stream()

        inference_success = (
            self.context.execute_async_v3(
                stream_handle=int(
                    stream.cuda_stream
                )
            )
        )

        if not inference_success:
            raise RuntimeError(
                "TensorRT Re-ID 추론 실패"
            )

        stream.synchronize()

        embedding = (
            output_tensor
            .float()
            .cpu()
            .numpy()
            .reshape(-1)
        )

        if embedding.size != 512:
            raise RuntimeError(
                f"Embedding 크기가 512가 아닙니다: "
                f"{embedding.shape}"
            )

        if not np.all(
            np.isfinite(embedding)
        ):
            raise RuntimeError(
                "Embedding에 NaN 또는 Inf가 있습니다."
            )

        return embedding

    def extract_from_file(
        self,
        image_path: str | Path,
    ) -> np.ndarray:
        image_path = Path(image_path)

        image = cv2.imread(
            str(image_path)
        )

        if image is None:
            raise RuntimeError(
                f"이미지를 읽지 못했습니다: {image_path}"
            )

        return self.extract(image)