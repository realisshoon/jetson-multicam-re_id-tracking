from __future__ import annotations

import json
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


class LiveStackScriptContractTest(unittest.TestCase):
    def read(self, name: str) -> str:
        return (SCRIPTS / name).read_text(encoding="utf-8")

    def test_start_contract_and_order_without_destructive_actions(self) -> None:
        script = self.read("start_live_stack.ps1")
        self.assertIn("$PSScriptRoot", script)
        self.assertIn("CCTV_TEST_ADMIN_TOKEN", script)
        self.assertIn("$env:MAIN_ADMIN_TOKEN = $AdminToken", script)
        self.assertIn('"$Name.pid.json"', script)
        self.assertIn("-RedirectStandardOutput", script)
        self.assertIn("-RedirectStandardError", script)
        self.assertIn("Wait-ApiHealth", script)
        self.assertIn("Wait-DatabaseStatus", script)
        self.assertIn("integrity_check", script)
        self.assertIn("New-LiveProcessRecord", script)
        self.assertIn("OwningProcess", script)
        self.assertNotIn("pid = $Process.Id", script)

        broker = script.index("$CurrentService = 'Broker'")
        main = script.index("$CurrentService = 'Main'")
        api = script.index("$CurrentService = 'API'")
        database = script.index("$CurrentService = 'DB'")
        self.assertLess(broker, main)
        self.assertLess(main, api)
        self.assertLess(api, database)

        lowered = script.lower()
        self.assertNotIn("reset/execute", lowered)
        self.assertNotIn("camera_a", lowered)
        self.assertNotIn("camera_b", lowered)
        self.assertNotIn("camera_c", lowered)
        self.assertNotIn("camera_d", lowered)

    def test_stop_contract_targets_only_recorded_pid_in_order(self) -> None:
        script = self.read("stop_live_stack.ps1")
        common = self.read("live_stack_common.ps1")
        self.assertIn("@('api','main','broker')", script)
        self.assertIn("Test-LiveProcessRecord", script)
        self.assertIn("Stop-Process -Id ([int]$Record.pid)", script)
        self.assertIn("Endpoint owners after stop", script)
        self.assertIn("EXECUTABLE_MISMATCH", common)
        self.assertIn("COMMAND_LINE_MISMATCH", common)
        self.assertIn("CREATION_TIME_MISMATCH", common)
        self.assertIn("ENDPOINT_OWNER_MISMATCH", common)
        self.assertIn("ToUniversalTime", common)
        self.assertIn("LiveStackCreationTimeToleranceSeconds", common)
        self.assertNotIn("Get-Process python", script)
        self.assertNotIn("Get-Process mosquitto", script)

    def test_status_contract_has_health_admin_and_recent_errors(self) -> None:
        script = self.read("status_live_stack.ps1")
        self.assertIn("/api/health", script)
        self.assertIn("/api/admin/database/status", script)
        self.assertIn("-Tail 10", script)
        self.assertIn("CCTV_TEST_ADMIN_TOKEN", script)
        self.assertIn("1883", script)
        self.assertIn("8080", script)
        self.assertIn("8091", script)
        self.assertIn("Broker loopback (unmanaged)", script)

    def test_readme_documents_commands_and_artifacts(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        for command in (
            ".\\scripts\\start_live_stack.ps1",
            ".\\scripts\\status_live_stack.ps1",
            ".\\scripts\\stop_live_stack.ps1",
        ):
            self.assertIn(command, readme)
        self.assertIn("data/run/*.pid.json", readme)
        self.assertIn("data/logs", readme)
        self.assertIn("integrity_check=ok", readme)

    @staticmethod
    def free_port() -> int:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            return int(listener.getsockname()[1])

    def test_wrapper_child_listener_pid_and_dual_address_stop(self) -> None:
        fixture = ROOT / "tests" / "fixtures" / "live_stack"
        ports: set[int] = set()
        while len(ports) < 3:
            ports.add(self.free_port())
        child_port, dual_port, unused_api_port = ports
        with tempfile.TemporaryDirectory() as temp_root:
            result = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(fixture / "verify_listener_ownership.ps1"),
                    "-ProjectRoot",
                    str(ROOT),
                    "-PythonPath",
                    sys.executable,
                    "-ListenerPath",
                    str(fixture / "tcp_listener.py"),
                    "-WrapperPath",
                    str(fixture / "wrapper_spawns_listener.ps1"),
                    "-TempRoot",
                    temp_root,
                    "-ChildPort",
                    str(child_port),
                    "-DualPort",
                    str(dual_port),
                    "-UnusedApiPort",
                    str(unused_api_port),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=60,
                check=True,
            )
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["wrapper_child_differ"])
        self.assertNotEqual(payload["wrapper_pid"], payload["listener_pid"])
        self.assertTrue(payload["executable_recorded"])
        self.assertTrue(payload["command_line_recorded"])
        self.assertTrue(payload["creation_time_utc"].endswith("Z"))
        self.assertEqual(payload["creation_time_adjustment_seconds"], 1)
        self.assertTrue(payload["child_listener_stopped"])
        self.assertTrue(payload["lan_listener_stopped"])
        self.assertTrue(payload["loopback_listener_alive"])
        self.assertNotEqual(payload["loopback_pid"], payload["lan_pid"])


if __name__ == "__main__":
    unittest.main()
