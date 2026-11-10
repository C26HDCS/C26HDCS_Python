"""One-second durable file heartbeat and five-calendar-year log retention."""

import atexit
from datetime import datetime, date
import gzip
import hashlib
import logging
import os
from pathlib import Path
import re
import shutil
import threading
import time
import uuid
from zoneinfo import ZoneInfo


ZONE = ZoneInfo('Asia/Seoul')
ARCHIVE = re.compile(r'^hdcs-python-\d+-[0-9a-f]{32}\.(\d{4}-\d{2}-\d{2})(?:\.\d+)?\.log\.gz$')


def cutoff(today):
    try:
        return today.replace(year=today.year - 5)
    except ValueError:
        return today.replace(year=today.year - 5, day=28)


class CalendarLogHandler(logging.Handler):
    def __init__(self, directory, clock=None):
        super().__init__()
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.clock = clock or (lambda: datetime.now(ZONE))
        self.prefix = f'hdcs-python-{os.getpid()}-{uuid.uuid4().hex}'
        self.day = None
        self.path = None
        self.stream = None
        self.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S KST'))
        self.formatter.converter = lambda seconds: time.gmtime(seconds + 9 * 3600)
        self._open(self.clock().date())

    def _open(self, day):
        self.day = day
        self.sequence = getattr(self, 'sequence', 0) + 1
        self.path = self.directory / f'{self.prefix}.{day.isoformat()}.{self.sequence}.log'
        self.stream = self.path.open('a', encoding='utf-8', newline='\n')

    def emit(self, record):
        try:
            current = self.clock().date()
            if self.day != current:
                old = self.path
                self.sync()
                self.stream.close()
                self._open(current)
                try:
                    self.compress(old)
                    self.prune(current)
                except OSError:
                    # The original remains if archiving fails; keep writing today's log.
                    self.stream.write('LOG_ARCHIVE_FAILURE original retained\n')
            self.stream.write(self.format(record) + '\n')
            self.stream.flush()
        except Exception:
            self.handleError(record)

    def sync(self):
        with self.lock:
            if self.stream is None or self.stream.closed:
                raise OSError('Log file is not writable')
            self.stream.flush()
            os.fsync(self.stream.fileno())

    @staticmethod
    def compress(source):
        destination = source.with_name(source.name + '.gz')
        if destination.exists():
            raise OSError('Archive already exists; preserving source')
        temporary = destination.with_name(destination.name + '.' + uuid.uuid4().hex + '.tmp')
        digest = hashlib.sha256()
        try:
            with source.open('rb') as incoming, temporary.open('xb') as raw:
                with gzip.GzipFile(fileobj=raw, mode='wb', mtime=0) as outgoing:
                    while chunk := incoming.read(1024 * 1024):
                        digest.update(chunk)
                        outgoing.write(chunk)
                raw.flush()
                os.fsync(raw.fileno())
            check = hashlib.sha256()
            with gzip.open(temporary, 'rb') as archived:
                while chunk := archived.read(1024 * 1024):
                    check.update(chunk)
            if digest.digest() != check.digest():
                raise OSError('Archive hash differs')
            os.replace(temporary, destination)
            source.unlink()
        finally:
            temporary.unlink(missing_ok=True)

    def prune(self, today=None):
        before = cutoff(today or self.clock().date())
        for path in self.directory.iterdir():
            match = ARCHIVE.fullmatch(path.name)
            if not match or path.is_symlink() or not path.is_file():
                continue
            try:
                archive_day = date.fromisoformat(match[1])
            except ValueError:
                continue
            # Keep the complete boundary day; never apply a size-based early purge.
            if archive_day < before:
                path.unlink(missing_ok=True)

    def close(self):
        with self.lock:
            if self.stream is not None and not self.stream.closed:
                self.sync()
                self.stream.close()
        super().close()


class Heartbeat:
    def __init__(self, log, handler, interval=1.0):
        self.log, self.handler, self.interval = log, handler, interval
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, name='log-heartbeat', daemon=True)
        self.thread.start()

    def run(self):
        previous = time.monotonic()
        counter = 0
        while not self.stop_event.wait(self.interval):
            current = time.monotonic()
            gap = current - previous
            previous = current
            counter += 1
            try:
                if gap > self.interval + 0.5:
                    self.log.error('LOG_INTERVAL_GAP elapsed_ms=%d', gap * 1000)
                self.log.info('HEARTBEAT component=Python pid=%s tick=%s', os.getpid(), counter)
                self.handler.sync()
                if counter % 60 == 0:
                    self.handler.prune()
                    if shutil.disk_usage(self.handler.directory).free < 1073741824:
                        self.log.error('LOG_DISK_LOW free_bytes<1073741824; retention remains five years')
            except Exception:
                # Do not silently terminate periodic storage on an I/O failure.
                import sys
                print('LOG_PERSISTENCE_FAILURE inspect disk and permissions', file=sys.stderr)

    def stop(self):
        self.stop_event.set()
        if threading.current_thread() != self.thread:
            self.thread.join(3)
        self.handler.sync()


def start_heartbeat(log, handler):
    heartbeat = Heartbeat(log, handler)
    atexit.register(heartbeat.stop)
    return heartbeat
