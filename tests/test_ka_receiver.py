import os
from unittest.mock import patch
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ka_receiver.receiver import KaReceiver


def setUpModule():
    mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite'})
    mode.start()
    unittest.addModuleCleanup(mode.stop)


class KaReceiverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.watch = self.root / "incoming"
        self.watch.mkdir()
        self.archive = self.root / "archive"
        self.database = self.root / "receipts.sqlite3"

    def make_receiver(self, settle_seconds: float = 0) -> KaReceiver:
        return KaReceiver(
            self.watch, self.archive, self.database, "cam_01",
            settle_seconds=settle_seconds,
        )

    def test_marker_completes_matching_video_once(self) -> None:
        video = self.watch / "clip.mp4"
        video.write_bytes(b"test video bytes")
        (self.watch / "clip.dump").write_text("done")
        with self.make_receiver() as receiver:
            result = receiver.scan_once()[0]
            self.assertEqual(result.status, "RECEIVED")
            self.assertEqual(receiver.scan_once()[0].status, "ALREADY_RECEIVED")
        db = sqlite3.connect(self.database)
        try:
            rows = db.execute(
                "SELECT camera_id, archive_path, byte_size, sha256 FROM ka_video_receipts"
            ).fetchall()
        finally:
            db.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "cam_01")
        self.assertEqual(Path(rows[0][1]).read_bytes(), video.read_bytes())
        self.assertEqual(Path(rows[0][1]).name, video.name)
        self.assertEqual(rows[0][2], video.stat().st_size)
        self.assertEqual(len(rows[0][3]), 64)

    def test_no_marker_does_not_receive(self) -> None:
        (self.watch / "clip.mp4").write_bytes(b"partial")
        with self.make_receiver() as receiver:
            self.assertEqual(receiver.scan_once(), [])

    def test_marker_without_video_waits(self) -> None:
        (self.watch / "clip.dump").touch()
        with self.make_receiver() as receiver:
            self.assertEqual(receiver.scan_once()[0].status, "WAITING")

    def test_recent_files_wait_until_settled(self) -> None:
        video = self.watch / "clip.mp4"
        marker = self.watch / "clip.dump"
        video.write_bytes(b"video")
        marker.touch()
        with self.make_receiver(settle_seconds=30) as receiver:
            self.assertEqual(receiver.scan_once()[0].status, "WAITING")
            old = video.stat().st_mtime - 60
            os.utime(video, (old, old))
            os.utime(marker, (old, old))
            self.assertEqual(receiver.scan_once()[0].status, "RECEIVED")

    def test_ambiguous_video_is_not_imported(self) -> None:
        (self.watch / "clip.mp4").write_bytes(b"one")
        (self.watch / "clip.mkv").write_bytes(b"two")
        (self.watch / "clip.dump").touch()
        with self.make_receiver() as receiver:
            self.assertEqual(receiver.scan_once()[0].status, "ERROR")
        self.assertEqual(list((self.archive / "cam_01").iterdir()), [])

    def test_marker_recreated_for_new_video_is_new_receipt(self) -> None:
        video = self.watch / "clip.mp4"
        marker = self.watch / "clip.dump"
        video.write_bytes(b"first")
        marker.touch()
        with self.make_receiver() as receiver:
            self.assertEqual(receiver.scan_once()[0].status, "RECEIVED")
            video.write_bytes(b"second")
            new_time_ns = marker.stat().st_mtime_ns - 1_000_000_000
            os.utime(marker, ns=(new_time_ns, new_time_ns))
            self.assertEqual(receiver.scan_once()[0].status, "RECEIVED")
            count = receiver.connection.execute("SELECT COUNT(*) FROM ka_video_receipts").fetchone()[0]
            self.assertEqual(count, 2)
            paths = [Path(row[0]) for row in receiver.connection.execute(
                'SELECT archive_path FROM ka_video_receipts ORDER BY id')]
            self.assertEqual([path.name for path in paths], ['clip.mp4', 'clip.mp4'])
            self.assertNotEqual(paths[0].parent, paths[1].parent)
            self.assertEqual([path.read_bytes() for path in paths], [b'first', b'second'])

    def test_cli_imports_file_pair_once(self) -> None:
        (self.watch / "capture.testvideo").write_bytes(b"sample video payload")
        (self.watch / "capture.dump").touch()
        command = [
            sys.executable, "-B", "-m", "ka_receiver", "--watch-dir", str(self.watch),
            "--archive-dir", str(self.archive), "--db", str(self.database),
            "--camera-id", "cam_01", "--extensions", ".testvideo",
            "--settle-seconds", "0", "--once",
        ]
        first = subprocess.run(command, capture_output=True, text=True, check=True)
        second = subprocess.run(command, capture_output=True, text=True, check=True)
        self.assertIn('"status": "RECEIVED"', first.stdout)
        self.assertNotIn('"status": "RECEIVED"', second.stdout)


if __name__ == "__main__":
    unittest.main()
