import json
import os
from unittest.mock import patch
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from dcs_receiver.__main__ import SAMPLE_HEX
from dcs_receiver.receiver import Ingestor
from dcs_receiver.sender import DcsSender
from dcs_receiver.storage import FrameStore


SAMPLE = bytes.fromhex(SAMPLE_HEX)


class RecordingHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.requests.append((self.path, dict(self.headers), json.loads(body)))
        status = self.server.responses.pop(0) if self.server.responses else 200
        self.send_response(status)
        if status == 302:
            self.send_header("Location", "/moved")
        self.end_headers()

    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_: object) -> None:
        pass


def setUpModule():
    mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite'})
    mode.start()
    unittest.addModuleCleanup(mode.stop)


class SenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "frames.sqlite3"
        self.server = HTTPServer(("127.0.0.1", 0), RecordingHandler)
        self.server.requests = []
        self.server.responses = []
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/dcs"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=3)
        self.server.server_close()
        self.directory.cleanup()

    def receive(self, source: str, data: bytes = SAMPLE) -> None:
        with FrameStore(self.database) as store:
            Ingestor(store, source).feed(data)

    def test_send_once_then_restart_does_not_send_duplicate(self) -> None:
        self.receive("tcp:127.0.0.1:45000")
        with DcsSender(
            self.database, destination_id="archive", url=self.url
        ) as sender:
            first = sender.send_ready()
            second = sender.send_ready()
        self.assertEqual((first.enqueued, first.sent, first.retry_scheduled), (1, 1, 0))
        self.assertEqual((second.enqueued, second.sent), (0, 0))

        with DcsSender(
            self.database, destination_id="archive", url=self.url
        ) as sender:
            self.assertEqual(sender.send_ready().sent, 0)

        self.assertEqual(len(self.server.requests), 1)
        path, headers, payload = self.server.requests[0]
        self.assertEqual(path, "/dcs")
        self.assertEqual(headers["Idempotency-Key"], payload["delivery_id"])
        self.assertEqual(payload["station"], {"number": "01", "system_group": "44", "group": "56"})
        self.assertEqual(payload["quantities"], ["0443", "0060", "0445", "0000"])
        self.assertEqual(payload["raw_hex"], SAMPLE.hex().upper())
        self.assertFalse(payload["quality"]["document_xor_matches"])

        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status, attempts FROM dcs_delivery_jobs"
            ).fetchone()
            attempts = connection.execute("SELECT count(*) FROM dcs_delivery_attempts").fetchone()[0]
        self.assertEqual(row, ("SENT", 1))
        self.assertEqual(attempts, 1)

    def test_failure_is_recorded_and_retry_uses_same_delivery_id(self) -> None:
        self.receive("tcp:127.0.0.1:45000")
        self.server.responses = [503, 200]
        with DcsSender(
            self.database,
            destination_id="archive",
            url=self.url,
            retry_base_seconds=30,
        ) as sender:
            failed = sender.send_ready()
            self.assertEqual((failed.sent, failed.retry_scheduled), (0, 1))
            self.assertEqual(sender.send_ready().sent, 0)

            with closing(sqlite3.connect(self.database)) as connection:
                row = connection.execute(
                    "SELECT status, attempts, response_code, next_attempt_at "
                    "FROM dcs_delivery_jobs"
                ).fetchone()
                self.assertEqual(row[:3], ("RETRY", 1, 503))
                self.assertIsNotNone(row[3])
                connection.execute(
                    "UPDATE dcs_delivery_jobs SET next_attempt_at = '2000-01-01T00:00:00+00:00'"
                )
                connection.commit()

            retried = sender.send_ready()
            self.assertEqual((retried.sent, retried.retry_scheduled), (1, 0))

        self.assertEqual(len(self.server.requests), 2)
        first_id = self.server.requests[0][1]["Idempotency-Key"]
        second_id = self.server.requests[1][1]["Idempotency-Key"]
        self.assertEqual(first_id, second_id)
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status, attempts FROM dcs_delivery_jobs"
            ).fetchone()
            outcomes = connection.execute(
                "SELECT outcome FROM dcs_delivery_attempts ORDER BY id"
            ).fetchall()
        self.assertEqual(row, ("SENT", 2))
        self.assertEqual(outcomes, [("RETRY",), ("SENT",)])

    def test_sample_and_invalid_frames_are_not_sent_by_default(self) -> None:
        self.receive("pdf-sample")
        invalid = bytearray(SAMPLE)
        invalid[13] = ord("A")
        self.receive("tcp:127.0.0.1:45000", bytes(invalid))

        with DcsSender(
            self.database, destination_id="archive", url=self.url
        ) as sender:
            summary = sender.send_ready()
        self.assertEqual((summary.enqueued, summary.sent), (0, 0))

        with DcsSender(
            self.database, destination_id="archive", url=self.url, source_prefix=None
        ) as sender:
            summary = sender.send_ready()
        self.assertEqual((summary.enqueued, summary.sent), (1, 1))
        self.assertEqual(len(self.server.requests), 1)

    def test_redirect_is_not_mistaken_for_success(self) -> None:
        self.receive("tcp:127.0.0.1:45000")
        self.server.responses = [302]
        with DcsSender(
            self.database, destination_id="archive", url=self.url
        ) as sender:
            summary = sender.send_ready()
        self.assertEqual((summary.sent, summary.retry_scheduled), (0, 1))
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status, response_code FROM dcs_delivery_jobs"
            ).fetchone()
        self.assertEqual(row, ("RETRY", 302))

    def test_cli_sends_sample_to_local_http_server(self) -> None:
        self.receive("pdf-sample")
        project_dir = Path(__file__).resolve().parent.parent
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "dcs_receiver",
                "--db",
                str(self.database),
                "send",
                "--destination-id",
                "local-test",
                "--url",
                self.url,
                "--once",
                "--all-sources",
            ],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        self.assertEqual(json.loads(result.stdout)["sent"], 1)
        self.assertEqual(len(self.server.requests), 1)


if __name__ == "__main__":
    unittest.main()
