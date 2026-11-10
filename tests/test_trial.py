import sqlite3
import os
from unittest.mock import patch
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path

from dcs_receiver.__main__ import make_tcp_server
from dcs_receiver.monitor import DcsMonitor
from dcs_receiver.sender import DcsSender
from dcs_receiver.test_sink import make_test_sink


def setUpModule():
    mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite'})
    mode.start()
    unittest.addModuleCleanup(mode.stop)


class TrialTests(unittest.TestCase):
    def test_sample_tcp_monitor_and_local_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "trial.sqlite3"
            received = []
            with make_tcp_server(database, "127.0.0.1", 0) as tcp_server:
                with make_test_sink(0, received.append) as sink_server:
                    tcp_thread = threading.Thread(target=tcp_server.serve_forever)
                    sink_thread = threading.Thread(target=sink_server.serve_forever)
                    tcp_thread.start()
                    sink_thread.start()
                    try:
                        project_dir = Path(__file__).resolve().parent.parent
                        subprocess.run(
                            [
                                sys.executable,
                                "-B",
                                "-m",
                                "dcs_receiver",
                                "sample-tcp",
                                "--port",
                                str(tcp_server.server_address[1]),
                            ],
                            cwd=project_dir,
                            capture_output=True,
                            text=True,
                            timeout=10,
                            check=True,
                        )

                        deadline = time.monotonic() + 3
                        while True:
                            if database.exists():
                                with closing(sqlite3.connect(database)) as connection:
                                    try:
                                        count = connection.execute(
                                            "SELECT count(*) FROM dcs_frames"
                                        ).fetchone()[0]
                                    except sqlite3.OperationalError:
                                        count = 0
                                if count == 1:
                                    break
                            if time.monotonic() >= deadline:
                                self.fail("sample frame was not stored by TCP receiver")
                            time.sleep(0.01)

                        self.assertEqual(DcsMonitor(database).check().snapshot.status, "RECEIVING")
                        with DcsSender(
                            database,
                            destination_id="local-trial",
                            url=f"http://127.0.0.1:{sink_server.server_port}/dcs",
                        ) as sender:
                            self.assertEqual(sender.send_ready().sent, 1)
                        self.assertEqual(len(received), 1)
                        self.assertEqual(received[0]["station"]["number"], "01")
                    finally:
                        tcp_server.shutdown()
                        sink_server.shutdown()
                        tcp_thread.join(timeout=3)
                        sink_thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
