from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import cv2
import torch
from ultralytics import YOLO


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = PROJECT_ROOT / "yolo26n.pt"

CAMERA_DEVICE = 0
SERVER_PORT = 8000

latest_jpeg: bytes | None = None
frame_lock = threading.Lock()


class StreamHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        # PC 브라우저에서 접속하는 메인 페이지
        if self.path == "/":
            html = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>Jetson CCTV Tracking</title>
    <style>
        body {
            margin: 0;
            background: #111;
            color: white;
            text-align: center;
            font-family: Arial, sans-serif;
        }

        h2 {
            margin: 15px;
        }

        img {
            width: 90%;
            max-width: 1280px;
            border: 2px solid white;
        }
    </style>
</head>
<body>
    <h2>Jetson C270 실시간 사람 추적</h2>
    <img src="/stream">
</body>
</html>
"""
            encoded = html.encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
            return

        # 실제 MJPEG 영상 스트림
        if self.path == "/stream":
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "multipart/x-mixed-replace; boundary=frame",
            )
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Pragma", "no-cache")
            self.end_headers()

            try:
                while True:
                    with frame_lock:
                        frame_data = latest_jpeg

                    if frame_data is None:
                        time.sleep(0.05)
                        continue

                    self.wfile.write(b"--frame\r\n")
                    self.wfile.write(b"Content-Type: image/jpeg\r\n")
                    self.wfile.write(
                        f"Content-Length: {len(frame_data)}\r\n\r\n".encode()
                    )
                    self.wfile.write(frame_data)
                    self.wfile.write(b"\r\n")
                    self.wfile.flush()

                    time.sleep(0.01)

            except (
                BrokenPipeError,
                ConnectionResetError,
                ConnectionAbortedError,
            ):
                pass

            return

        self.send_error(404)

    def log_message(self, format, *args):
        return


def start_web_server():
    server = ThreadingHTTPServer(
        ("0.0.0.0", SERVER_PORT),
        StreamHandler,
    )

    print(f"웹 서버 실행: http://10.10.20.56:{SERVER_PORT}")
    server.serve_forever()


def main():
    global latest_jpeg

    if not torch.cuda.is_available():
        raise RuntimeError("Jetson GPU를 사용할 수 없습니다.")

    print("GPU:", torch.cuda.get_device_name(0))

    model = YOLO(str(MODEL_PATH))

    cap = cv2.VideoCapture(CAMERA_DEVICE, cv2.CAP_V4L2)

    if not cap.isOpened():
        raise RuntimeError("/dev/video0 카메라를 열 수 없습니다.")

    cap.set(
        cv2.CAP_PROP_FOURCC,
        cv2.VideoWriter_fourcc(*"MJPG"),
    )
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
    cap.set(cv2.CAP_PROP_FPS, 30)

    server_thread = threading.Thread(
        target=start_web_server,
        daemon=True,
    )
    server_thread.start()

    print("종료: Ctrl + C")

    try:
        while True:
            success, frame = cap.read()

            if not success:
                print("카메라 프레임 읽기 실패")
                continue

            # results = model.track(
            #     source=frame,
            #     persist=True,
            #     tracker="bytetrack.yaml",
            #     classes=[0],
            #     conf=0.25,
            #     device=0,
            #     verbose=False,
            # )

            results = model.track(
                source=frame,
                persist=True,
                tracker="bytetrack.yaml",
                classes=[0],
                conf=0.50,
                iou=0.50,
                end2end=False,
                device=0,
                verbose=False,
            )

            result = results[0]
            annotated_frame = result.plot()

            track_ids = []

            if result.boxes is not None and result.boxes.id is not None:
                track_ids = result.boxes.id.int().cpu().tolist()

            cv2.putText(
                annotated_frame,
                f"Track IDs: {track_ids}",
                (20, 35),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.8,
                (0, 255, 0),
                2,
            )

            encode_success, buffer = cv2.imencode(
                ".jpg",
                annotated_frame,
                [cv2.IMWRITE_JPEG_QUALITY, 80],
            )

            if not encode_success:
                continue

            with frame_lock:
                latest_jpeg = buffer.tobytes()

            print(
                f"\r현재 사람 ID: {track_ids}",
                end="",
                flush=True,
            )

    except KeyboardInterrupt:
        print("\n스트리밍 종료")

    finally:
        cap.release()


if __name__ == "__main__":
    main()