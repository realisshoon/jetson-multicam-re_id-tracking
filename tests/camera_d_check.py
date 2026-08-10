from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import cv2


# ============================================================
# Camera D 설정
# ============================================================

CAMERA_DEVICE = 4

CAMERA_WIDTH = 1280
CAMERA_HEIGHT = 720
CAMERA_FPS = 30

SERVER_PORT = 8002

FLIP_HORIZONTAL = True


# ============================================================
# 공유 프레임
# ============================================================

latest_jpeg: bytes | None = None
frame_lock = threading.Lock()


# ============================================================
# 웹 서버
# ============================================================

class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class StreamHandler(BaseHTTPRequestHandler):

    def do_GET(self) -> None:
        if self.path == "/":
            html = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>Camera D Check</title>

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
            width: 95%;
            max-width: 1280px;
            border: 3px solid #00ff00;
        }
    </style>
</head>

<body>
    <h2>Camera D - /dev/video4</h2>
    <img src="/stream">
</body>
</html>
"""
            data = html.encode("utf-8")

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8",
            )
            self.send_header(
                "Content-Length",
                str(len(data)),
            )
            self.end_headers()

            self.wfile.write(data)
            return

        if self.path == "/stream":
            self.send_response(200)

            self.send_header(
                "Content-Type",
                "multipart/x-mixed-replace; boundary=frame",
            )
            self.send_header(
                "Cache-Control",
                "no-cache",
            )
            self.send_header(
                "Pragma",
                "no-cache",
            )
            self.end_headers()

            try:
                while True:
                    with frame_lock:
                        frame_data = latest_jpeg

                    if frame_data is None:
                        time.sleep(0.05)
                        continue

                    self.wfile.write(b"--frame\r\n")
                    self.wfile.write(
                        b"Content-Type: image/jpeg\r\n"
                    )
                    self.wfile.write(
                        (
                            f"Content-Length: "
                            f"{len(frame_data)}\r\n\r\n"
                        ).encode()
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

    def log_message(self, format, *args) -> None:
        return


def start_web_server() -> None:
    server = ReusableThreadingHTTPServer(
        ("0.0.0.0", SERVER_PORT),
        StreamHandler,
    )

    print(
        f"Camera D 웹 서버: "
        f"http://10.10.20.56:{SERVER_PORT}"
    )

    server.serve_forever()


# ============================================================
# 메인
# ============================================================

def main() -> None:
    global latest_jpeg

    cap = cv2.VideoCapture(
        CAMERA_DEVICE,
        cv2.CAP_V4L2,
    )

    if not cap.isOpened():
        raise RuntimeError(
            f"/dev/video{CAMERA_DEVICE} "
            f"카메라를 열 수 없습니다."
        )

    cap.set(
        cv2.CAP_PROP_FOURCC,
        cv2.VideoWriter_fourcc(*"MJPG"),
    )

    cap.set(
        cv2.CAP_PROP_FRAME_WIDTH,
        CAMERA_WIDTH,
    )

    cap.set(
        cv2.CAP_PROP_FRAME_HEIGHT,
        CAMERA_HEIGHT,
    )

    cap.set(
        cv2.CAP_PROP_FPS,
        CAMERA_FPS,
    )

    actual_width = int(
        cap.get(cv2.CAP_PROP_FRAME_WIDTH)
    )

    actual_height = int(
        cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
    )

    actual_fps = cap.get(
        cv2.CAP_PROP_FPS
    )

    server_thread = threading.Thread(
        target=start_web_server,
        daemon=True,
    )

    server_thread.start()

    print("Camera D 테스트 시작")
    print(
        f"카메라 장치 : /dev/video{CAMERA_DEVICE}"
    )
    print(
        f"해상도      : "
        f"{actual_width} x {actual_height}"
    )
    print(
        f"요청 FPS    : {CAMERA_FPS}"
    )
    print(
        f"카메라 FPS  : {actual_fps:.1f}"
    )
    print(
        f"웹 주소     : "
        f"http://10.10.20.56:{SERVER_PORT}"
    )
    print("종료         : Ctrl + C")

    try:
        while True:
            success, frame = cap.read()

            if not success:
                print(
                    "Camera D 프레임 읽기 실패"
                )
                time.sleep(0.05)
                continue

            if FLIP_HORIZONTAL:
                frame = cv2.flip(
                    frame,
                    1,
                )

            cv2.putText(
                frame,
                "CAMERA D CHECK",
                (20, 40),
                cv2.FONT_HERSHEY_SIMPLEX,
                1.0,
                (0, 255, 0),
                2,
            )

            cv2.putText(
                frame,
                "/dev/video4 - PORT 8002",
                (20, 80),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.70,
                (255, 255, 255),
                2,
            )

            encode_success, buffer = cv2.imencode(
                ".jpg",
                frame,
                [
                    cv2.IMWRITE_JPEG_QUALITY,
                    80,
                ],
            )

            if not encode_success:
                continue

            with frame_lock:
                latest_jpeg = buffer.tobytes()

    except KeyboardInterrupt:
        print()
        print("Camera D 테스트 종료")

    finally:
        cap.release()


if __name__ == "__main__":
    main()