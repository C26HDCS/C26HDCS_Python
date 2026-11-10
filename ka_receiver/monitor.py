"""Monitor receiver progress, completed videos, and incomplete file pairs."""

import argparse
import json
import hdcs_db as db
import hdcs_telemetry as telemetry
import time
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .telemetry import initialize


@dataclass(frozen=True)
class Snapshot:
    camera_id: str
    checked_at: str
    receiver_status: str
    data_status: str
    pending_status: str
    last_heartbeat_at: str | None
    last_received_at: str | None
    seconds_since_received: float | None
    pending_count: int
    oldest_pending_at: str | None
    error_count: int
    last_error: str | None
    heartbeat_timeout_seconds: int
    stale_after_seconds: int
    pending_after_seconds: int


@dataclass(frozen=True)
class MonitorCheck:
    snapshot: Snapshot
    changed: bool


class KaMonitor:
    def __init__(self, database: Path, camera_id: str, *, heartbeat_timeout_seconds: int = 30,
                 stale_after_seconds: int = 600, pending_after_seconds: int = 300) -> None:
        if not camera_id or min(heartbeat_timeout_seconds, stale_after_seconds, pending_after_seconds) < 1:
            raise ValueError('camera_id and positive timeout values are required')
        self.database = database
        self.camera_id = camera_id
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.stale_after_seconds = stale_after_seconds
        self.pending_after_seconds = pending_after_seconds
        with closing(db.connect(database, timeout=10)) as connection:
            initialize(connection)
            connection.initialize('''CREATE TABLE IF NOT EXISTS ka_monitor_status (
                camera_id TEXT PRIMARY KEY, signature TEXT NOT NULL, snapshot_json TEXT NOT NULL
            )''')
            connection.initialize('''CREATE TABLE IF NOT EXISTS ka_monitor_events (
                id INTEGER PRIMARY KEY, camera_id TEXT NOT NULL, changed_at TEXT NOT NULL,
                snapshot_json TEXT NOT NULL
            )''')
            connection.commit()

    @telemetry.runtime_guard('KA', 'KA_MONITOR_RUNTIME')
    def check(self, now: datetime | None = None) -> MonitorCheck:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError('now must include a timezone')
        now = now.astimezone(timezone.utc)

        def age(timestamp: str | None) -> float | None:
            return max(0, (now - datetime.fromisoformat(timestamp)).total_seconds()) if timestamp else None

        with closing(db.connect(self.database, timeout=10)) as connection:
            connection.row_factory = db.Row
            connection.execute('BEGIN IMMEDIATE')
            runtime = connection.execute(
                'SELECT * FROM ka_receiver_runtime WHERE camera_id = ?', (self.camera_id,)
            ).fetchone()
            receipt = None
            if connection.table_exists('ka_video_receipts'):
                receipt = connection.execute(
                    'SELECT received_at FROM ka_video_receipts WHERE camera_id = ? ORDER BY id DESC LIMIT 1',
                    (self.camera_id,),
                ).fetchone()
            pending = connection.execute(
                'SELECT COUNT(*), MIN(first_seen_at) FROM ka_receiver_pending WHERE camera_id = ?',
                (self.camera_id,),
            ).fetchone()
            heartbeat = runtime['heartbeat_at'] if runtime else None
            error_count = runtime['error_count'] if runtime else 0
            if runtime is None:
                receiver_status = 'NEVER_STARTED'
            elif runtime['state'] == 'STOPPED':
                receiver_status = 'STOPPED'
            elif age(heartbeat) > self.heartbeat_timeout_seconds:
                receiver_status = 'UNRESPONSIVE'
            elif error_count:
                receiver_status = 'ERROR'
            else:
                receiver_status = 'RUNNING'
            received_at = receipt[0] if receipt else None
            received_age = age(received_at)
            data_status = ('NO_DATA' if received_age is None else
                           'STALE' if received_age > self.stale_after_seconds else 'RECEIVING')
            pending_status = ('NONE' if pending[0] == 0 else
                              'DELAYED' if age(pending[1]) > self.pending_after_seconds else 'WAITING')
            snapshot = Snapshot(
                self.camera_id, now.isoformat(timespec='seconds'), receiver_status,
                data_status, pending_status, heartbeat, received_at, received_age,
                pending[0], pending[1], error_count, runtime['last_error'] if runtime else None,
                self.heartbeat_timeout_seconds, self.stale_after_seconds, self.pending_after_seconds,
            )
            payload = json.dumps(asdict(snapshot), ensure_ascii=False)
            signature = json.dumps([receiver_status, data_status, pending_status, error_count, snapshot.last_error])
            previous = connection.execute(
                'SELECT signature FROM ka_monitor_status WHERE camera_id = ?', (self.camera_id,)
            ).fetchone()
            changed = previous is None or previous[0] != signature
            connection.execute(
                '''INSERT INTO ka_monitor_status VALUES (?, ?, ?)
                   ON CONFLICT(camera_id) DO UPDATE SET signature = excluded.signature,
                   snapshot_json = excluded.snapshot_json''', (self.camera_id, signature, payload),
            )
            if changed:
                connection.execute(
                    'INSERT INTO ka_monitor_events (camera_id, changed_at, snapshot_json) VALUES (?, ?, ?)',
                    (self.camera_id, snapshot.checked_at, payload),
                )
            device = telemetry.device_for(connection, 'KA', self.camera_id)
            if connection.is_postgres:
                from hdcs_status import publish, ttl_seconds
                health = ('ERROR' if receiver_status != 'RUNNING' else
                          'WARNING' if data_status != 'RECEIVING' or pending_status == 'DELAYED' else 'NORMAL')
                publish(connection, device, 'KA', 'MONITOR', self.camera_id, health,
                        f'receiver={receiver_status} data={data_status} pending={pending_status}', ttl=ttl_seconds())
            if changed:
                telemetry.record(connection, device, 'KA_MONITOR',
                                 f'camera={self.camera_id} receiver={receiver_status} data={data_status} pending={pending_status}')
            telemetry.condition(connection, device, 'KA_RECEIVER_STATUS', self.camera_id,
                                receiver_status != 'RUNNING',
                                f'camera={self.camera_id} receiver={receiver_status}')
            telemetry.condition(connection, device, 'KA_DATA_STATUS', self.camera_id,
                                data_status != 'RECEIVING',
                                f'camera={self.camera_id} data={data_status}',
                                category=telemetry.DATA, severity='MEDIUM')
            telemetry.condition(connection, device, 'KA_PENDING_DELAY', self.camera_id,
                                pending_status == 'DELAYED',
                                f'camera={self.camera_id} pending={pending_status}',
                                category=telemetry.DATA, severity='MEDIUM')
            connection.commit()
            return MonitorCheck(snapshot, changed)


def main() -> None:
    parser = argparse.ArgumentParser(description='Monitor Ka CCTV file reception for one camera')
    parser.add_argument('--db', type=Path, default=Path(__file__).resolve().parent.parent / 'data' / 'ka_receipts.sqlite3')
    parser.add_argument('--camera-id', required=True)
    parser.add_argument('--heartbeat-timeout-seconds', type=int, default=30)
    parser.add_argument('--stale-after-seconds', type=int, default=600)
    parser.add_argument('--pending-after-seconds', type=int, default=300)
    parser.add_argument('--interval-seconds', type=int, default=5)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if args.interval_seconds < 1:
        parser.error('interval-seconds must be positive')
    try:
        monitor = KaMonitor(args.db, args.camera_id,
                            heartbeat_timeout_seconds=args.heartbeat_timeout_seconds,
                            stale_after_seconds=args.stale_after_seconds,
                            pending_after_seconds=args.pending_after_seconds)
        first = True
        while True:
            check = monitor.check()
            if first or check.changed or args.once:
                print(json.dumps(asdict(check.snapshot), ensure_ascii=False), flush=True)
            if args.once:
                return
            first = False
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        pass
    except (ValueError, OSError, db.Error) as exc:
        telemetry.report_failure(args.db, 'KA', 'KA_MONITOR_RUNTIME', key=args.camera_id, error=exc)
        parser.exit(1, f'Monitor error: {exc}\n')


if __name__ == '__main__':
    main()
