from __future__ import annotations

import json
import unittest

from src.server.mqtt_capture import format_capture


class MqttCaptureTest(unittest.TestCase):
    def test_embedding_is_summarized_instead_of_dumped(self) -> None:
        embedding = [0.1, 0.2, 0.3, 0.4]
        output = format_capture(
            "cctv/entry",
            json.dumps({"journey_id": "j-1", "embedding": embedding}).encode(),
        )
        self.assertIn("received_at=", output)
        self.assertIn("topic=cctv/entry", output)
        self.assertIn('"length": 4', output)
        self.assertNotIn("[0.1, 0.2, 0.3, 0.4]", output)

    def test_invalid_json_is_visible_without_raising(self) -> None:
        output = format_capture("cctv/passage/b", b"not-json")
        self.assertIn("invalid_json=", output)
        self.assertIn("not-json", output)


if __name__ == "__main__":
    unittest.main()
