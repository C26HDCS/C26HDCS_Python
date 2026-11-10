import sqlite3
import os
from unittest.mock import patch
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dcs_receiver.monitor import DcsMonitor
from dcs_receiver.storage import FrameStore


NOW = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)


def insert_frame(path: Path, *, source: str, status: str, received_at: datetime) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "INSERT INTO dcs_frames (received_at, source, raw_hex, parse_status) "
            "VALUES (?, ?, ?, ?)",
            (received_at.isoformat(timespec="milliseconds"), source, "020D", status),
        )
        connection.commit()


def setUpModule():
    mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite'})
    mode.start()
    unittest.addModuleCleanup(mode.stop)


class MonitorTests(unittest.TestCase):
    def test_no_data_and_sample_is_excluded_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.sqlite3"
            monitor = DcsMonitor(path, stale_after_seconds=60)
            first = monitor.check(NOW)
            self.assertEqual(first.snapshot.status, "NO_DATA")
            self.assertTrue(first.changed)

            insert_frame(path, source="pdf-sample", status="PARSED", received_at=NOW)
            second = monitor.check(NOW)
            self.assertEqual(second.snapshot.status, "NO_DATA")
            self.assertFalse(second.changed)

            including_samples = DcsMonitor(path, stale_after_seconds=60, source_prefix=None)
            self.assertEqual(including_samples.check(NOW).snapshot.status, "RECEIVING")

    def test_receiving_stale_and_invalid_only_transitions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.sqlite3"
            with FrameStore(path):
                pass
            insert_frame(
                path,
                source="tcp:127.0.0.1:45000",
                status="PARSED",
                received_at=NOW - timedelta(seconds=30),
            )
            monitor = DcsMonitor(path, stale_after_seconds=60)
            receiving = monitor.check(NOW)
            self.assertEqual(receiving.snapshot.status, "RECEIVING")
            self.assertEqual(receiving.snapshot.seconds_since_valid, 30)
            self.assertFalse(monitor.check(NOW + timedelta(seconds=1)).changed)

            stale = monitor.check(NOW + timedelta(seconds=120))
            self.assertEqual(stale.snapshot.status, "STALE")
            self.assertTrue(stale.changed)

            insert_frame(
                path,
                source="tcp:127.0.0.1:45000",
                status="INVALID",
                received_at=NOW + timedelta(seconds=120),
            )
            invalid_only = monitor.check(NOW + timedelta(seconds=121))
            self.assertEqual(invalid_only.snapshot.status, "INVALID_ONLY")
            self.assertEqual(invalid_only.snapshot.seconds_since_received, 1)
            self.assertTrue(invalid_only.changed)

            with closing(sqlite3.connect(path)) as connection:
                current = connection.execute(
                    "SELECT status FROM dcs_monitor_status WHERE source_filter = 'tcp:'"
                ).fetchone()
                events = connection.execute(
                    "SELECT status FROM dcs_monitor_events WHERE source_filter = 'tcp:' ORDER BY id"
                ).fetchall()
            self.assertEqual(current, ("INVALID_ONLY",))
            self.assertEqual(events, [("RECEIVING",), ("STALE",), ("INVALID_ONLY",)])


if __name__ == "__main__":
    unittest.main()
