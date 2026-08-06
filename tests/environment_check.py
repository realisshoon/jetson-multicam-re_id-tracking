from __future__ import annotations

import platform
import subprocess
import sys
from importlib import metadata
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

EXPECTED_VERSIONS = {
    "numpy": "1.26.4",
    "ultralytics": "8.4.112",
    "lap": "0.5.12",
    "PyYAML": "6.0.2",
    "paho-mqtt": "2.1.0",
}


def print_title(title: str) -> None:
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def run_command(command: list[str]) -> str:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )

        output = result.stdout.strip()

        if not output:
            output = result.stderr.strip()

        return output if output else "확인되지 않음"

    except FileNotFoundError:
        return f"{command[0]} 명령어 없음"


def get_package_version(package_name: str) -> str | None:
    try:
        return metadata.version(package_name)

    except metadata.PackageNotFoundError:
        return None


def check_normal_packages() -> None:
    print_title("일반 Python 패키지")

    for package_name, expected_version in EXPECTED_VERSIONS.items():
        installed_version = get_package_version(package_name)

        if installed_version is None:
            print(
                f"[미설치] {package_name:<15} "
                f"필요 버전: {expected_version}"
            )
            continue

        if installed_version == expected_version:
            status = "정상"
        else:
            status = "버전 다름"

        print(
            f"[{status}] {package_name:<15} "
            f"설치: {installed_version} / "
            f"기준: {expected_version}"
        )


def check_opencv() -> None:
    print_title("OpenCV")

    try:
        import cv2

        print(f"OpenCV 버전: {cv2.__version__}")
        print(f"OpenCV 경로: {cv2.__file__}")

        build_info = cv2.getBuildInformation()

        gstreamer_line = next(
            (
                line.strip()
                for line in build_info.splitlines()
                if "GStreamer:" in line
            ),
            "GStreamer 정보 없음",
        )

        print(gstreamer_line)

    except Exception as error:
        print(f"[오류] OpenCV import 실패: {error}")


def check_torch() -> None:
    print_title("PyTorch / CUDA")

    try:
        import torch

        print(f"PyTorch 버전: {torch.__version__}")
        print(f"PyTorch 경로: {torch.__file__}")
        print(f"PyTorch CUDA 버전: {torch.version.cuda}")
        print(f"CUDA 사용 가능: {torch.cuda.is_available()}")

        if torch.cuda.is_available():
            print(f"GPU 개수: {torch.cuda.device_count()}")
            print(f"GPU 이름: {torch.cuda.get_device_name(0)}")
            print(f"cuDNN 버전: {torch.backends.cudnn.version()}")
        else:
            print("[주의] PyTorch가 Jetson GPU를 사용하지 못하고 있습니다.")

    except Exception as error:
        print(f"[오류] PyTorch import 실패: {error}")


def check_torchvision() -> None:
    print_title("Torchvision")

    try:
        import torchvision

        print(f"Torchvision 버전: {torchvision.__version__}")
        print(f"Torchvision 경로: {torchvision.__file__}")

    except Exception as error:
        print(f"[오류] Torchvision import 실패: {error}")


def check_tensorrt() -> None:
    print_title("TensorRT")

    try:
        import tensorrt as trt

        print(f"TensorRT 버전: {trt.__version__}")
        print(f"TensorRT 경로: {trt.__file__}")

    except Exception as error:
        print(f"[오류] TensorRT import 실패: {error}")


def main() -> None:
    print_title("프로젝트 및 Python 환경")

    print(f"프로젝트 경로: {PROJECT_ROOT}")
    print(f"Python 실행 파일: {sys.executable}")
    print(f"Python 버전: {sys.version.split()[0]}")
    print(f"CPU 아키텍처: {platform.machine()}")
    print(f"운영체제: {platform.platform()}")

    print_title("Jetson / JetPack")

    print("L4T 정보:")
    print(run_command(["cat", "/etc/nv_tegra_release"]))

    print()
    print("JetPack 패키지:")
    print(
        run_command(
            [
                "dpkg-query",
                "-W",
                "-f=${Package} ${Version}\n",
                "nvidia-jetpack",
            ]
        )
    )

    print()
    print("CUDA 컴파일러:")
    print(run_command(["nvcc", "--version"]))

    check_normal_packages()
    check_opencv()
    check_torch()
    check_torchvision()
    check_tensorrt()

    print_title("검사 완료")
    print("위 결과에서 미설치 또는 오류가 난 항목만 설치하면 됩니다.")


if __name__ == "__main__":
    main()