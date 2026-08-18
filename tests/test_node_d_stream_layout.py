from __future__ import annotations

import http.client
import threading
import unittest
from unittest.mock import patch

import cv2
import numpy as np

from src.nodes import node_d


class NodeDStreamLayoutTest(unittest.TestCase):
    def setUp(self) -> None:
        node_d.candidates.clear()
        node_d.completed_journey_ids.clear()
        node_d.terminal_journey_ids.clear()
        node_d.expired_journey_count = 0

    def test_stream_size_matches_camera_a_runtime_frame(self) -> None:
        self.assertEqual(
            (node_d.STREAM_WIDTH, node_d.STREAM_HEIGHT),
            (1280, 720),
        )

        source = np.full((480, 640, 3), 127, dtype=np.uint8)
        output = node_d.build_candidate_dashboard(source)
        encoded, jpeg = cv2.imencode(".jpg", output)
        self.assertTrue(encoded)
        decoded = cv2.imdecode(jpeg, cv2.IMREAD_COLOR)
        self.assertEqual(decoded.shape[:2], (720, 1280))

    def test_cover_resize_uses_center_crop_without_side_bars(self) -> None:
        source = np.full((480, 640, 3), (30, 90, 160), dtype=np.uint8)

        output, transform = node_d.resize_and_center_crop(source)

        self.assertEqual(output.shape, (720, 1280, 3))
        self.assertEqual(transform.scale_x, 2.0)
        self.assertEqual(transform.scale_y, 2.0)
        self.assertEqual(transform.crop_x, 0)
        self.assertEqual(transform.crop_y, 120)
        self.assertTrue(np.all(output[:, 0] == (30, 90, 160)))
        self.assertTrue(np.all(output[:, -1] == (30, 90, 160)))

    def test_display_render_does_not_modify_inference_frame(self) -> None:
        source = np.random.default_rng(7).integers(
            0,
            256,
            size=(480, 640, 3),
            dtype=np.uint8,
        )
        original = source.copy()
        annotations = [
            node_d.DisplayAnnotation(
                box=(510, 350, 639, 479),
                label="ANOMALY: STRANGER",
                detail="NO B PASSAGE CANDIDATE",
                color=(0, 0, 255),
            )
        ]

        output = node_d.build_candidate_dashboard(source, annotations)

        self.assertTrue(np.array_equal(source, original))
        self.assertEqual(output.shape, (720, 1280, 3))

    def test_right_edge_annotation_text_is_repositioned_inside_frame(self) -> None:
        source = np.zeros((480, 640, 3), dtype=np.uint8)
        output, transform = node_d.resize_and_center_crop(source)
        annotation = node_d.DisplayAnnotation(
            box=(600, 200, 639, 400),
            label="ANOMALY: STRANGER",
            detail="NO B PASSAGE CANDIDATE",
            color=(0, 0, 255),
        )

        with patch.object(
            node_d.cv2,
            "putText",
            wraps=node_d.cv2.putText,
        ) as put_text:
            node_d.draw_display_annotation(output, annotation, transform)

        self.assertEqual(put_text.call_count, 2)
        for call in put_text.call_args_list:
            text = call.args[1]
            x, y = call.args[2]
            font = call.args[3]
            scale = call.args[4]
            thickness = call.args[6]
            width, height = cv2.getTextSize(
                text,
                font,
                scale,
                thickness,
            )[0]
            self.assertGreaterEqual(x, 0)
            self.assertGreaterEqual(y - height, 0)
            self.assertLessEqual(x + width, node_d.STREAM_WIDTH)
            self.assertLessEqual(y, node_d.STREAM_HEIGHT)

    def test_anomaly_status_has_priority(self) -> None:
        annotations = [
            node_d.DisplayAnnotation(
                box=(0, 0, 10, 10),
                label="P000006 | ARRIVED",
                detail="A > B > D",
                color=(0, 255, 0),
            ),
            node_d.DisplayAnnotation(
                box=(20, 20, 30, 30),
                label="ANOMALY: STRANGER",
                detail="NO B PASSAGE CANDIDATE",
                color=(0, 0, 255),
            ),
        ]

        status, color = node_d.display_status(annotations)

        self.assertEqual(status, "ANOMALY DETECTED")
        self.assertEqual(color, (40, 60, 255))

    def test_display_status_with_person_and_journey_id_labels(self) -> None:
        arrived_annotation = [
            node_d.DisplayAnnotation(
                box=(0, 0, 10, 10),
                label="P000013 | J000017 | ARRIVED",
                detail="A > C > [D]",
                color=(0, 255, 0),
            )
        ]
        status, color = node_d.display_status(arrived_annotation)
        self.assertEqual(status, "STATUS: ARRIVAL")
        self.assertEqual(color, (40, 230, 80))

        checking_annotation = [
            node_d.DisplayAnnotation(
                box=(0, 0, 10, 10),
                label="CHECKING: P000013 | J000017",
                detail="BEST 0.77 TOP2 0.75 3/5",
                color=(0, 255, 255),
            )
        ]
        status, color = node_d.display_status(checking_annotation)
        self.assertEqual(status, "STATUS: VERIFYING")
        self.assertEqual(color, (0, 220, 255))

        verifying_annotation = [
            node_d.DisplayAnnotation(
                box=(0, 0, 10, 10),
                label="P000013 | J000017 | VERIFYING",
                detail="A > C > [D]",
                color=(0, 255, 255),
            )
        ]
        status, color = node_d.display_status(verifying_annotation)
        self.assertEqual(status, "STATUS: VERIFYING")
        self.assertEqual(color, (0, 220, 255))

    def test_live_hud_uses_required_label_and_red_indicator(self) -> None:
        source = np.full((480, 640, 3), 127, dtype=np.uint8)

        with patch.object(
            node_d.cv2,
            "putText",
            wraps=node_d.cv2.putText,
        ) as put_text:
            output = node_d.build_candidate_dashboard(source)

        labels = [call.args[1] for call in put_text.call_args_list]
        self.assertIn("LIVE | CAM D", labels)
        self.assertGreater(int(output[40, 34, 2]), 200)
        self.assertLess(int(output[40, 34, 0]), 100)

    def test_stream_route_serves_1280_by_720_jpeg(self) -> None:
        source = np.full((480, 640, 3), 127, dtype=np.uint8)
        output = node_d.build_candidate_dashboard(source)
        encoded, jpeg = cv2.imencode(
            ".jpg",
            output,
            [cv2.IMWRITE_JPEG_QUALITY, node_d.STREAM_JPEG_QUALITY],
        )
        self.assertTrue(encoded)
        node_d.latest_jpeg = jpeg.tobytes()
        server = node_d.ReusableServer(
            ("127.0.0.1", 0),
            node_d.StreamHandler,
        )
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.start()

        connection = http.client.HTTPConnection(
            "127.0.0.1",
            server.server_address[1],
            timeout=2,
        )
        try:
            connection.request("GET", "/stream")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(
                response.getheader("Content-Type"),
                "multipart/x-mixed-replace; boundary=frame",
            )
            self.assertEqual(response.fp.readline(), b"--frame\r\n")
            self.assertEqual(
                response.fp.readline(),
                b"Content-Type: image/jpeg\r\n",
            )
            length_line = response.fp.readline().decode("ascii").strip()
            content_length = int(length_line.split(":", 1)[1])
            self.assertEqual(response.fp.readline(), b"\r\n")
            frame_data = response.fp.read(content_length)
            decoded = cv2.imdecode(
                np.frombuffer(frame_data, dtype=np.uint8),
                cv2.IMREAD_COLOR,
            )
            self.assertEqual(decoded.shape[:2], (720, 1280))
        finally:
            connection.close()
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2)
            node_d.latest_jpeg = None


if __name__ == "__main__":
    unittest.main()
