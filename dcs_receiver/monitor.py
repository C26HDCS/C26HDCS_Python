"""Monitor whether live DCS frames continue to arrive."""

import hdcs_db as db
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .storage import CREATE_TABLE
import hdcs_telemetry as telemetry


CREATE_STATUS_TABLE = """
CREATE TABLE IF NOT EXISTS dcs_monitor_status (
    source_filter TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    stale_after_seconds INTEGER NOT NULL,
    last_received_at TEXT,
    last_valid_at TEXT
)
"""

CREATE_EVENT_TABLE = """
CREATE TABLE IF NOT EXISTS dcs_monitor_events (
    id INTEGER PRIMARY KEY,
    source_filter TEXT NOT NULL,
    status TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    last_received_at TEXT,
    last_valid_at TEXT
)
"""


@dataclass(frozen=True)
class MonitorSnapshot:
    status: str
    checked_at: str
    source_filter: str
    stale_after_seconds: int
    last_received_at: str | None
    last_valid_at: str | None
    seconds_since_received: int | None
    seconds_since_valid: int | None


@dataclass(frozen=True)
class MonitorCheck:
    snapshot: MonitorSnapshot
    changed: bool


class DcsMonitor:
    def __init__(
        self,
        database: Path,
        *,
        stale_after_seconds: int = 600,
        source_prefix: str | None = "tcp:",
        station_key: str | None = None,
    ) -> None:
        if stale_after_seconds < 1:
            raise ValueError("stale_after_seconds must be positive")
        if source_prefix == "":
            raise ValueError("source_prefix cannot be empty; use None for all sources")
        self.database = database
        self.stale_after_seconds = stale_after_seconds
        self.source_prefix = source_prefix
        if station_key is not None and (len(station_key.split('/')) != 3 or
                any(len(part) != 2 or not part.isascii() or not part.isdigit()
                    for part in station_key.split('/'))):
            raise ValueError('station_key must use system/group/station, for example 44/56/01')
        self.station_key = station_key
        with closing(db.connect(self.database, timeout=10)) as connection:
            connection.initialize(CREATE_TABLE)
            connection.initialize(CREATE_STATUS_TABLE)
            connection.initialize(CREATE_EVENT_TABLE)
            connection.initialize(
                "CREATE INDEX IF NOT EXISTS idx_dcs_frames_received ON dcs_frames(received_at DESC)"
            )
            connection.initialize(
                "CREATE INDEX IF NOT EXISTS idx_dcs_frames_parsed_received "
                "ON dcs_frames(parse_status, received_at DESC)"
            )
            connection.commit()

    @telemetry.runtime_guard('DCS', 'DCS_MONITOR_RUNTIME')
    def check(self, now: datetime | None = None) -> MonitorCheck:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError("now must include a timezone")
        now = now.astimezone(timezone.utc)
        source_filter = self.source_prefix or "*"
        if self.station_key:
            source_filter += '|station=' + self.station_key

        with closing(db.connect(self.database, timeout=10)) as connection:
            connection.execute('BEGIN IMMEDIATE')
            condition = "substr(source, 1, length(?)) = ?" if self.source_prefix else "1 = 1"
            parameters = (self.source_prefix, self.source_prefix) if self.source_prefix else ()
            condition += " AND (source IS NULL OR source <> 'dashboard-lband-sample')"
            if self.station_key:
                condition += ' AND system_group_number = ? AND group_number = ? AND station_number = ?'
                parameters += tuple(self.station_key.split('/'))
            last_received_at = connection.execute(
                f"SELECT received_at FROM dcs_frames WHERE {condition} "
                "ORDER BY received_at DESC, id DESC LIMIT 1",
                parameters,
            ).fetchone()
            mapped = 'AND dcs_data_id IS NOT NULL ' if connection.is_postgres else ''
            last_valid_at = connection.execute(
                f"SELECT received_at FROM dcs_frames WHERE {condition} "
                "AND parse_status = 'PARSED' " + mapped + "ORDER BY received_at DESC, id DESC LIMIT 1",
                parameters,
            ).fetchone()

            received = last_received_at[0] if last_received_at else None
            valid = last_valid_at[0] if last_valid_at else None
            received_age = _age_seconds(now, received)
            valid_age = _age_seconds(now, valid)

            if received_age is None:
                status = "NO_DATA"
            elif received_age > self.stale_after_seconds:
                status = "STALE"
            elif valid_age is None or valid_age > self.stale_after_seconds:
                status = "INVALID_ONLY"
            else:
                status = "RECEIVING"

            snapshot = MonitorSnapshot(
                status=status,
                checked_at=now.isoformat(timespec="seconds"),
                source_filter=source_filter,
                stale_after_seconds=self.stale_after_seconds,
                last_received_at=received,
                last_valid_at=valid,
                seconds_since_received=int(received_age) if received_age is not None else None,
                seconds_since_valid=int(valid_age) if valid_age is not None else None,
            )
            previous = connection.execute(
                "SELECT status FROM dcs_monitor_status WHERE source_filter = ?",
                (source_filter,),
            ).fetchone()
            changed = previous is None or previous[0] != status
            connection.execute(
                """
                INSERT INTO dcs_monitor_status (
                    source_filter, status, checked_at, stale_after_seconds,
                    last_received_at, last_valid_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_filter) DO UPDATE SET
                    status = excluded.status,
                    checked_at = excluded.checked_at,
                    stale_after_seconds = excluded.stale_after_seconds,
                    last_received_at = excluded.last_received_at,
                    last_valid_at = excluded.last_valid_at
                """,
                (source_filter, status, snapshot.checked_at, self.stale_after_seconds, received, valid),
            )
            if changed:
                connection.execute(
                    """
                    INSERT INTO dcs_monitor_events (
                        source_filter, status, changed_at, last_received_at, last_valid_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (source_filter, status, snapshot.checked_at, received, valid),
                )
            device = telemetry.device_for(connection, 'DCS', self.station_key)
            if connection.is_postgres:
                from hdcs_status import publish, monitor_scope, ttl_seconds
                scope = monitor_scope(connection, device, 'DCS', self.station_key)
                if scope:
                    publish(connection, device, 'DCS', 'MONITOR', scope,
                            'NORMAL' if status == 'RECEIVING' else 'ERROR' if status == 'INVALID_ONLY' else 'WARNING',
                            f'data={status}', ttl=ttl_seconds())
            if changed:
                telemetry.record(connection, device, 'DCS_MONITOR', f'{source_filter} status={status}')
            telemetry.condition(connection, device, 'DCS_DATA_STATUS', source_filter,
                                status != 'RECEIVING', f'{source_filter} status={status}',
                                category=telemetry.DATA, severity='MEDIUM', owner_key=self.station_key)
            connection.commit()
            return MonitorCheck(snapshot, changed)


def _age_seconds(now: datetime, timestamp: str | None) -> float | None:
    if timestamp is None:
        return None
    received_at = datetime.fromisoformat(timestamp)
    return max(0.0, (now - received_at).total_seconds())
