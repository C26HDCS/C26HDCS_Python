import json
import os
from unittest.mock import patch
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ka_receiver.monitor import KaMonitor
from ka_receiver.receiver import KaReceiver


def setUpModule():
    mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite'})
    mode.start()
    unittest.addModuleCleanup(mode.stop)


class KaMonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.incoming = self.root / 'incoming'
        self.incoming.mkdir()
        self.db = self.root / 'ka.sqlite3'
        self.monitor = KaMonitor(self.db, 'cam01')

    def receiver(self):
        return KaReceiver(self.incoming, self.root / 'archive', self.db, 'cam01', settle_seconds=0)

    def test_lifecycle_receipts_and_camera_isolation(self):
        snapshot = self.monitor.check().snapshot
        self.assertEqual((snapshot.receiver_status, snapshot.data_status), ('NEVER_STARTED', 'NO_DATA'))
        with self.receiver() as receiver:
            receiver.scan_once()
            self.assertEqual(self.monitor.check().snapshot.receiver_status, 'RUNNING')
            (self.incoming / 'clip.mp4').write_bytes(b'test bytes')
            (self.incoming / 'clip.dump').touch()
            receiver.scan_once()
            self.assertEqual(self.monitor.check().snapshot.data_status, 'RECEIVING')
            other = KaMonitor(self.db, 'cam02').check().snapshot
            self.assertEqual((other.receiver_status, other.data_status), ('NEVER_STARTED', 'NO_DATA'))
            future = datetime.now(timezone.utc) + timedelta(seconds=601)
            delayed = self.monitor.check(future).snapshot
            self.assertEqual((delayed.receiver_status, delayed.data_status), ('UNRESPONSIVE', 'STALE'))
        self.assertEqual(self.monitor.check().snapshot.receiver_status, 'STOPPED')

    def test_missing_dump_delays_then_recovers(self):
        (self.incoming / 'clip.mp4').write_bytes(b'test bytes')
        with self.receiver() as receiver:
            receiver.scan_once()
            initial = self.monitor.check().snapshot
            self.assertEqual((initial.pending_count, initial.pending_status), (1, 'WAITING'))
            self.assertEqual(initial.data_status, 'NO_DATA')
            receiver.scan_once()
            self.assertEqual(self.monitor.check().snapshot.oldest_pending_at, initial.oldest_pending_at)
            future = datetime.now(timezone.utc) + timedelta(seconds=301)
            self.assertEqual(self.monitor.check(future).snapshot.pending_status, 'DELAYED')
            (self.incoming / 'clip.dump').touch()
            receiver.scan_once()
            completed = self.monitor.check().snapshot
            self.assertEqual((completed.pending_status, completed.data_status), ('NONE', 'RECEIVING'))

    def test_processing_error_and_folder_error_recover(self):
        (self.incoming / 'clip.dump').touch()
        (self.incoming / 'clip.mp4').write_bytes(b'one')
        (self.incoming / 'clip.mkv').write_bytes(b'two')
        with self.receiver() as receiver:
            receiver.scan_once()
            self.assertEqual(self.monitor.check().snapshot.receiver_status, 'ERROR')
            (self.incoming / 'clip.mkv').unlink()
            receiver.scan_once()
            self.assertEqual(self.monitor.check().snapshot.receiver_status, 'RUNNING')
            moved = self.root / 'offline'
            self.incoming.rename(moved)
            receiver.scan_once()
            error = self.monitor.check().snapshot
            self.assertEqual(error.receiver_status, 'ERROR')
            self.assertTrue(error.last_error)
            moved.rename(self.incoming)
            receiver.scan_once()
            self.assertEqual(self.monitor.check().snapshot.receiver_status, 'RUNNING')

    def test_monitor_restart_does_not_duplicate_events(self):
        self.assertTrue(self.monitor.check().changed)
        self.assertFalse(KaMonitor(self.db, 'cam01').check().changed)

    def test_cli_once_outputs_snapshot(self):
        result = subprocess.run(
            [sys.executable, '-B', '-m', 'ka_receiver.monitor', '--db', str(self.db),
             '--camera-id', 'cam01', '--once'],
            cwd=Path(__file__).resolve().parent.parent, capture_output=True, text=True, check=True,
        )
        self.assertEqual(json.loads(result.stdout)['receiver_status'], 'NEVER_STARTED')


if __name__ == '__main__':
    unittest.main()
