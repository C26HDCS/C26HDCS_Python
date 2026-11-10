"""Import CCTV files only after a matching completion marker arrives."""

import hashlib
import os
import hdcs_db as db
import hdcs_telemetry as telemetry
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .telemetry import initialize
from .filenames import original_filename


DEFAULT_EXTENSIONS = frozenset({".mp4", ".mkv", ".ts", ".avi", ".mov", ".dav"})


@dataclass(frozen=True)
class ScanResult:
    marker: str
    status: str
    detail: str = ""
    record_id: int | None = None


class KaReceiver:
    def __init__(
        self,
        watch_dir: Path,
        archive_dir: Path,
        database: Path,
        camera_id: str,
        *,
        settle_seconds: float = 2,
        extensions: frozenset[str] = DEFAULT_EXTENSIONS,
    ) -> None:
        if not watch_dir.is_dir():
            raise ValueError(f"watch directory does not exist: {watch_dir}")
        if not camera_id or len(camera_id) > 64 or not all(
            char.isascii() and (char.isalnum() or char in "-_") for char in camera_id
        ):
            raise ValueError("camera_id must contain 1-64 ASCII letters, digits, - or _")
        if settle_seconds < 0:
            raise ValueError("settle_seconds must not be negative")
        if not extensions or any(not ext.startswith(".") or ext != ext.lower() for ext in extensions):
            raise ValueError("extensions must be lowercase file extensions")

        self.watch_dir = watch_dir.resolve()
        self.database = database
        self.archive_dir = (archive_dir / camera_id).resolve()
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.connection = db.connect(database, timeout=10)
        self.connection.initialize(
            """
            CREATE TABLE IF NOT EXISTS ka_video_receipts (
                id INTEGER PRIMARY KEY,
                received_at TEXT NOT NULL,
                camera_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                marker_path TEXT NOT NULL,
                marker_mtime_ns INTEGER NOT NULL,
                marker_size INTEGER NOT NULL,
                archive_path TEXT NOT NULL,
                byte_size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                UNIQUE(marker_path, marker_mtime_ns, marker_size)
            )
            """
        )
        self.connection.commit()
        self.camera_id = camera_id
        self.settle_seconds = settle_seconds
        self.extensions = extensions
        initialize(self.connection)
        self.connection.execute(
            """INSERT INTO ka_receiver_runtime (camera_id, watch_dir, heartbeat_at, state)
               VALUES (?, ?, ?, 'RUNNING') ON CONFLICT(camera_id) DO UPDATE SET
               watch_dir = excluded.watch_dir, heartbeat_at = excluded.heartbeat_at,
               state = 'RUNNING', error_count = 0, last_error = NULL""",
            (self.camera_id, str(self.watch_dir), datetime.now(timezone.utc).isoformat()),
        )
        self.connection.commit()
        self._last_pulse = time.monotonic()
        with self.connection:
            device = telemetry.device_for(self.connection, 'KA', self.camera_id)
            telemetry.record(self.connection, device, 'KA_RECEIVER', f'camera={self.camera_id} started')

    def close(self) -> None:
        try:
            self.connection.rollback()
            self.connection.execute(
                "UPDATE ka_receiver_runtime SET state = 'STOPPED' WHERE camera_id = ?",
                (self.camera_id,),
            )
            self.connection.commit()
            with self.connection:
                telemetry.record(self.connection, telemetry.device_for(self.connection, 'KA', self.camera_id),
                                 'KA_RECEIVER', f'camera={self.camera_id} stopped')
        finally:
            self.connection.close()

    def __enter__(self) -> "KaReceiver":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @telemetry.runtime_guard('KA', 'KA_RECEIVER_RUNTIME')
    def scan_once(self) -> list[ScanResult]:
        self._pulse(force=True)
        try:
            results = self._scan_once()
            pending = {r.marker: r.detail for r in results if r.status == 'WAITING'}
            entries = list(self.watch_dir.iterdir())
            marker_stems = {p.stem for p in entries if p.suffix.lower() == '.dump' and p.is_file()}
            for path in entries:
                if path.is_file() and path.suffix.lower() in self.extensions and path.stem not in marker_stems:
                    pending['video:' + path.name] = 'completion marker not found'
        except OSError as exc:
            self.connection.rollback()
            self.connection.execute(
                """UPDATE ka_receiver_runtime SET error_count = 1, last_error = ?
                   WHERE camera_id = ?""", (str(exc), self.camera_id),
            )
            telemetry.condition(self.connection, telemetry.device_for(self.connection, 'KA', self.camera_id),
                                'KA_RECEIVE_ERROR', self.camera_id, True,
                                f'camera={self.camera_id} folder access failed: {type(exc).__name__}')
            self.connection.commit()
            return [ScanResult('', 'ERROR', str(exc))]

        now = datetime.now(timezone.utc).isoformat()
        errors = [r.detail for r in results if r.status == 'ERROR']
        with self.connection:
            old_items = self.connection.execute(
                'SELECT item FROM ka_receiver_pending WHERE camera_id = ?', (self.camera_id,)
            ).fetchall()
            for (item,) in old_items:
                if item not in pending:
                    self.connection.execute(
                        'DELETE FROM ka_receiver_pending WHERE camera_id = ? AND item = ?',
                        (self.camera_id, item),
                    )
            for item, detail in pending.items():
                self.connection.execute(
                    """INSERT INTO ka_receiver_pending VALUES (?, ?, ?, ?)
                       ON CONFLICT(camera_id, item) DO UPDATE SET detail = excluded.detail""",
                    (self.camera_id, item, now, detail),
                )
            self.connection.execute(
                """UPDATE ka_receiver_runtime SET heartbeat_at = ?, error_count = ?, last_error = ?
                   WHERE camera_id = ?""",
                (now, len(errors), errors[0] if errors else None, self.camera_id),
            )
            telemetry.condition(self.connection, telemetry.device_for(self.connection, 'KA', self.camera_id),
                                'KA_RECEIVE_ERROR', self.camera_id, bool(errors),
                                f'camera={self.camera_id} receive errors={len(errors)}')
        return results

    def _pulse(self, *, force: bool = False) -> None:
        if force or time.monotonic() - self._last_pulse >= 2:
            self.connection.execute(
                'UPDATE ka_receiver_runtime SET heartbeat_at = ? WHERE camera_id = ?',
                (datetime.now(timezone.utc).isoformat(), self.camera_id),
            )
            self.connection.commit()
            self._last_pulse = time.monotonic()

    def _scan_once(self) -> list[ScanResult]:
        results = []
        markers = sorted(
            path for path in self.watch_dir.iterdir()
            if path.suffix.lower() == ".dump" and path.is_file()
        )
        for marker in markers:
            self._pulse()
            try:
                results.append(self._receive(marker))
            except (OSError, db.Error) as exc:
                self.connection.rollback()
                results.append(ScanResult(marker.name, "ERROR", str(exc)))
        return results

    def _receive(self, marker: Path) -> ScanResult:
        if marker.is_symlink() or not marker.stem:
            return ScanResult(marker.name, "ERROR", "invalid marker")
        marker_before = marker.stat()
        event_key = (str(marker.resolve()), marker_before.st_mtime_ns, marker_before.st_size)
        existing = self.connection.execute(
            """SELECT id FROM ka_video_receipts
               WHERE marker_path = ? AND marker_mtime_ns = ? AND marker_size = ?""",
            event_key,
        ).fetchone()
        if existing:
            return ScanResult(marker.name, "ALREADY_RECEIVED", record_id=existing[0])

        matches = [
            path for path in self.watch_dir.iterdir()
            if path.is_file() and not path.is_symlink()
            and path.stem == marker.stem and path.suffix.lower() in self.extensions
        ]
        if not matches:
            return ScanResult(marker.name, "WAITING", "matching video not found")
        if len(matches) > 1:
            return ScanResult(marker.name, "ERROR", "multiple videos match marker")

        source = matches[0]
        source_before = source.stat()
        now_ns = time.time_ns()
        settle_ns = self.settle_seconds * 1_000_000_000
        if source_before.st_size == 0 or (
            now_ns - source_before.st_mtime_ns < settle_ns
            or now_ns - marker_before.st_mtime_ns < settle_ns
        ):
            return ScanResult(marker.name, "WAITING", "files are empty or still settling")

        try:
            filename = original_filename(source.name)
        except ValueError:
            return ScanResult(marker.name, 'ERROR', 'invalid original filename')
        receipt_dir = self.archive_dir / uuid.uuid4().hex
        receipt_dir.mkdir()
        destination = receipt_dir / filename
        staging = receipt_dir / '.incoming.part'
        digest = hashlib.sha256()
        copied_size = 0
        try:
            with source.open("rb") as incoming, staging.open("xb") as outgoing:
                while chunk := incoming.read(1024 * 1024):
                    outgoing.write(chunk)
                    digest.update(chunk)
                    copied_size += len(chunk)
                    self._pulse()
                outgoing.flush()
                os.fsync(outgoing.fileno())

            source_after = source.stat()
            marker_after = marker.stat()
            if (
                copied_size != source_before.st_size
                or (source_after.st_size, source_after.st_mtime_ns)
                != (source_before.st_size, source_before.st_mtime_ns)
                or (marker_after.st_size, marker_after.st_mtime_ns)
                != (marker_before.st_size, marker_before.st_mtime_ns)
            ):
                return ScanResult(marker.name, "WAITING", "file changed during copy")

            os.replace(staging, destination)
            try:
                values = (
                    datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    self.camera_id, str(source.resolve()), *event_key,
                    str(destination), copied_size, digest.hexdigest(),
                )
                if self.connection.is_postgres:
                    row_id = db.save_ka(self.connection, values)
                else:
                    row_id = self._save_sqlite_receipt(values)
            except (OSError, db.Error):
                self.connection.rollback()
                destination.unlink(missing_ok=True)
                raise
            return ScanResult(marker.name, "RECEIVED", source.name, row_id)
        finally:
            staging.unlink(missing_ok=True)
            if not destination.exists():
                receipt_dir.rmdir()

    def _save_sqlite_receipt(self, values):
        cursor = self.connection.execute(
            """INSERT INTO ka_video_receipts (
                received_at, camera_id, source_path, marker_path,
                marker_mtime_ns, marker_size, archive_path, byte_size, sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            values,
        )
        self.connection.commit()
        with self.connection:
            telemetry.record(self.connection, telemetry.device_for(self.connection, 'KA', self.camera_id),
                             'KA_RECEIVE', f'record={cursor.lastrowid} bytes={values[7]} received')
        return cursor.lastrowid
