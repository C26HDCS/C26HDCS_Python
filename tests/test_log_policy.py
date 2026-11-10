from datetime import date, datetime
import gzip
import logging
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from hdcs_log_policy import CalendarLogHandler, Heartbeat, ZONE, cutoff


class LogPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.current = datetime(2026, 10, 23, 12, tzinfo=ZONE)
        self.handler = CalendarLogHandler(self.root, lambda: self.current)
        self.addCleanup(self.handler.close)
        self.log = logging.Logger('policy-test')
        self.log.addHandler(self.handler)

    def test_daily_rotation_compresses_and_preserves_original_content(self):
        self.log.info('first day')
        original = self.handler.path
        self.current = datetime(2026, 10, 24, 0, 0, 1, tzinfo=ZONE)
        self.log.info('next day')
        archive = original.with_name(original.name + '.gz')
        self.assertFalse(original.exists())
        with gzip.open(archive, 'rt', encoding='utf-8') as stream:
            self.assertIn('first day', stream.read())
        self.assertIn('next day', self.handler.path.read_text(encoding='utf-8'))

    def test_no_size_or_backup_count_limit_deletes_recent_logs(self):
        for index in range(8):
            self.current = datetime(2026, 10, 23 + index, 12, tzinfo=ZONE)
            self.log.info('keep every day')
        self.assertEqual(len(list(self.root.glob('*.gz'))), 7)

    def test_retention_boundary_leap_day_and_unrelated_files(self):
        prefix = 'hdcs-python-1-' + 'a' * 32
        old = self.root / f'{prefix}.2021-10-22.log.gz'
        boundary = self.root / f'{prefix}.2021-10-23.log.gz'
        keep = self.root / 'unrelated.2020-01-01.log.gz'
        for path in (old, boundary, keep):
            path.touch()
        self.handler.prune(date(2026, 10, 23))
        self.assertFalse(old.exists())
        self.assertTrue(boundary.exists())
        self.assertTrue(keep.exists())
        self.assertEqual(cutoff(date(2028, 2, 29)), date(2023, 2, 28))

    def test_compression_failure_does_not_remove_source_or_stop_new_day(self):
        self.log.info('preserve me')
        original = self.handler.path
        self.current = datetime(2026, 10, 24, 12, tzinfo=ZONE)
        with patch.object(CalendarLogHandler, 'compress', side_effect=OSError('disk error')):
            self.log.info('still running')
        self.assertTrue(original.exists())
        self.assertIn('still running', self.handler.path.read_text(encoding='utf-8'))

    def test_heartbeat_creates_and_syncs_logs_without_business_input(self):
        written = threading.Event()
        real_sync = self.handler.sync
        def sync():
            real_sync()
            written.set()
        with patch.object(self.handler, 'sync', side_effect=sync):
            heartbeat = Heartbeat(self.log, self.handler)
            try:
                self.assertTrue(written.wait(4))
            finally:
                heartbeat.stop()
        self.assertIn('HEARTBEAT', self.handler.path.read_text(encoding='utf-8'))

    def test_pid_reuse_does_not_overwrite_existing_process_files(self):
        other = CalendarLogHandler(self.root, lambda: self.current)
        try:
            self.assertNotEqual(other.path, self.handler.path)
        finally:
            other.close()

    def test_clock_rollback_cannot_overwrite_an_earlier_day_archive(self):
        for day, message in [(23, 'first'), (24, 'second'), (23, 'third'), (24, 'fourth')]:
            self.current = datetime(2026, 10, day, 12, tzinfo=ZONE)
            self.log.info(message)
        contents = self.handler.path.read_text(encoding='utf-8')
        for archive in self.root.glob('*.gz'):
            with gzip.open(archive, 'rt', encoding='utf-8') as stream:
                contents += stream.read()
        for message in ('first', 'second', 'third', 'fourth'):
            self.assertIn(message, contents)
