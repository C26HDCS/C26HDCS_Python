"""Durable delivery queue and provisional HTTP transport for DCS frames."""

import json
import hdcs_db as db
import hdcs_telemetry as telemetry
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .storage import CREATE_TABLE


CREATE_CURSOR_TABLE = """
CREATE TABLE IF NOT EXISTS dcs_delivery_cursors (
    destination_id TEXT NOT NULL,
    source_filter TEXT NOT NULL,
    last_frame_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (destination_id, source_filter)
)
"""

CREATE_JOB_TABLE = """
CREATE TABLE IF NOT EXISTS dcs_delivery_jobs (
    id INTEGER PRIMARY KEY,
    frame_id INTEGER NOT NULL,
    destination_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    lease_until TEXT,
    last_error TEXT,
    response_code INTEGER,
    sent_at TEXT,
    UNIQUE(destination_id, frame_id),
    FOREIGN KEY(frame_id) REFERENCES dcs_frames(id)
)
"""

CREATE_ATTEMPT_TABLE = """
CREATE TABLE IF NOT EXISTS dcs_delivery_attempts (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    attempt_number INTEGER NOT NULL,
    attempted_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    response_code INTEGER,
    error TEXT,
    FOREIGN KEY(job_id) REFERENCES dcs_delivery_jobs(id)
)
"""


@dataclass(frozen=True)
class SendSummary:
    enqueued: int
    sent: int
    retry_scheduled: int


class DeliveryError(Exception):
    def __init__(self, message: str, response_code: int | None = None) -> None:
        super().__init__(message)
        self.response_code = response_code


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_: object) -> None:
        return None


_HTTP_OPENER = build_opener(_NoRedirect())


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def build_payload(job: dict) -> dict:
    return {
        "schema_version": 1,
        "delivery_id": job["delivery_id"],
        "frame_id": job["frame_id"],
        "received_at": job["received_at"],
        "source": job["source"],
        "station": {
            "number": job["station_number"],
            "system_group": job["system_group_number"],
            "group": job["group_number"],
        },
        "voltage": job["voltage"],
        "quantities": [
            job["quantity_1"],
            job["quantity_2"],
            job["quantity_3"],
            job["quantity_4"],
        ],
        "raw_hex": job["raw_hex"],
        "quality": {
            "document_xor_matches": (
                bool(job["document_xor_matches"])
                if job["document_xor_matches"] is not None else None
            ),
            "error_code_hex": job["error_code_hex"],
            "document_xor_hex": job["document_xor_hex"],
        },
    }


def post_json(url: str, payload: dict, delivery_id: str, timeout_seconds: int) -> int:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    request = Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Idempotency-Key": delivery_id,
        },
        method="POST",
    )
    try:
        with _HTTP_OPENER.open(request, timeout=timeout_seconds) as response:
            return response.status
    except HTTPError as exc:
        exc.close()
        raise DeliveryError(f"HTTP {exc.code}", exc.code) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise DeliveryError(str(exc)) from exc


class DcsSender:
    def __init__(
        self,
        database: Path,
        *,
        destination_id: str,
        url: str,
        source_prefix: str | None = "tcp:",
        timeout_seconds: int = 10,
        retry_base_seconds: int = 30,
        retry_max_seconds: int = 900,
    ) -> None:
        parsed_url = urlparse(url)
        if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
            raise ValueError("url must be an HTTP or HTTPS endpoint")
        if not destination_id.strip():
            raise ValueError("destination_id is required")
        if source_prefix == "":
            raise ValueError("source_prefix cannot be empty; use None for all sources")
        if min(timeout_seconds, retry_base_seconds, retry_max_seconds) < 1:
            raise ValueError("timeout and retry intervals must be positive")
        self.destination_id = destination_id
        self.database = database
        self.url = url
        self.source_prefix = source_prefix
        self.timeout_seconds = timeout_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self._connection = db.connect(database, timeout=10)
        self._connection.row_factory = db.Row
        self._connection.initialize("PRAGMA foreign_keys = ON")
        self._connection.initialize(CREATE_TABLE)
        self._connection.initialize(CREATE_CURSOR_TABLE)
        self._connection.initialize(CREATE_JOB_TABLE)
        self._connection.initialize(CREATE_ATTEMPT_TABLE)
        self._connection.initialize(
            "CREATE INDEX IF NOT EXISTS idx_dcs_delivery_ready "
            "ON dcs_delivery_jobs(destination_id, status, next_attempt_at, frame_id)"
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "DcsSender":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def enqueue_new(self, batch_size: int = 500) -> int:
        source_filter = self.source_prefix or "*"
        enqueued = 0
        while True:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._connection.execute(
                    "SELECT last_frame_id FROM dcs_delivery_cursors "
                    "WHERE destination_id = ? AND source_filter = ?",
                    (self.destination_id, source_filter),
                ).fetchone()
                last_id = cursor[0] if cursor else 0
                extra = ', dcs_data_id' if self._connection.is_postgres else ''
                rows = self._connection.execute(
                    f"SELECT id, source, parse_status{extra} FROM dcs_frames "
                    "WHERE id > ? ORDER BY id LIMIT ?",
                    (last_id, batch_size),
                ).fetchall()
                for row in rows:
                    if self._connection.is_postgres and row['dcs_data_id'] is None:
                        continue
                    if row["parse_status"] != "PARSED":
                        continue
                    if row["source"] == "dashboard-lband-sample":
                        continue
                    if self.source_prefix and not row["source"].startswith(self.source_prefix):
                        continue
                    inserted = self._connection.execute(
                        """
                        INSERT OR IGNORE INTO dcs_delivery_jobs (
                            frame_id, destination_id, delivery_id, status, next_attempt_at
                        ) VALUES (?, ?, ?, 'PENDING', ?)
                        """,
                        (row["id"], self.destination_id, uuid.uuid4().hex, _timestamp(_utc_now())),
                    )
                    enqueued += inserted.rowcount
                if rows:
                    self._connection.execute(
                        """
                        INSERT INTO dcs_delivery_cursors (
                            destination_id, source_filter, last_frame_id
                        ) VALUES (?, ?, ?)
                        ON CONFLICT(destination_id, source_filter) DO UPDATE SET
                            last_frame_id = excluded.last_frame_id
                        """,
                        (self.destination_id, source_filter, rows[-1]["id"]),
                    )
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
            if len(rows) < batch_size:
                return enqueued

    def _claim_next(self, now: datetime) -> dict | None:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._connection.execute(
                """
                SELECT id, attempts FROM dcs_delivery_jobs
                WHERE destination_id = ? AND (
                    status = 'PENDING'
                    OR (status = 'RETRY' AND next_attempt_at <= ?)
                    OR (status = 'SENDING' AND lease_until <= ?)
                )
                ORDER BY frame_id LIMIT 1
                """,
                (self.destination_id, _timestamp(now), _timestamp(now)),
            ).fetchone()
            if row is None:
                self._connection.commit()
                return None
            attempts = row["attempts"] + 1
            lease = now + timedelta(seconds=max(60, self.timeout_seconds * 2 + 5))
            self._connection.execute(
                "UPDATE dcs_delivery_jobs SET status = 'SENDING', attempts = ?, "
                "lease_until = ?, next_attempt_at = NULL WHERE id = ?",
                (attempts, _timestamp(lease), row["id"]),
            )
            job = self._connection.execute(
                """
                SELECT j.id AS job_id, j.frame_id, j.delivery_id, j.attempts,
                       f.received_at, f.source, f.raw_hex, f.station_number,
                       f.system_group_number, f.group_number, f.voltage,
                       f.quantity_1, f.quantity_2, f.quantity_3, f.quantity_4,
                       f.error_code_hex, f.document_xor_hex, f.document_xor_matches
                FROM dcs_delivery_jobs j
                JOIN dcs_frames f ON f.id = j.frame_id
                WHERE j.id = ?
                """,
                (row["id"],),
            ).fetchone()
            self._connection.commit()
            return dict(job)
        except Exception:
            self._connection.rollback()
            raise

    def _record_result(
        self,
        job: dict,
        *,
        now: datetime,
        response_code: int | None,
        error: str | None,
    ) -> None:
        if error is None:
            status = "SENT"
            next_attempt_at = None
        else:
            status = "RETRY"
            delay = min(
                self.retry_base_seconds * 2 ** min(job["attempts"] - 1, 30),
                self.retry_max_seconds,
            )
            next_attempt_at = _timestamp(now + timedelta(seconds=delay))
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            updated = self._connection.execute(
                """
                UPDATE dcs_delivery_jobs
                SET status = ?, next_attempt_at = ?, lease_until = NULL,
                    last_error = ?, response_code = ?, sent_at = ?
                WHERE id = ? AND status = 'SENDING' AND attempts = ?
                """,
                (
                    status,
                    next_attempt_at,
                    error[:1000] if error else None,
                    response_code,
                    _timestamp(now) if error is None else None,
                    job["job_id"],
                    job["attempts"],
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("delivery lease was claimed by another sender")
            self._connection.execute(
                """
                INSERT INTO dcs_delivery_attempts (
                    job_id, attempt_number, attempted_at, outcome, response_code, error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    job["job_id"],
                    job["attempts"],
                    _timestamp(now),
                    status,
                    response_code,
                    error[:1000] if error else None,
                ),
            )
            key = f"{job['system_group_number']}/{job['group_number']}/{job['station_number']}"
            device = telemetry.device_for(self._connection, 'DCS', key)
            telemetry.record(self._connection, device, 'DCS_SEND',
                             f"destination={self.destination_id} job={job['job_id']} attempt={job['attempts']} status={status} http={response_code}",
                             'ERROR' if error else 'INFO')
            telemetry.condition(self._connection, device, 'DCS_SEND_ERROR', self.destination_id,
                                error is not None, f'destination={self.destination_id} delivery={status} http={response_code}',
                                category=telemetry.NETWORK, owner_key=key)
            if self._connection.is_postgres:
                from hdcs_status import publish
                publish(self._connection, device, 'DCS', 'DELIVERY', str(job['job_id']),
                        'WARNING' if error else 'NORMAL', f'destination={self.destination_id} status={status}',
                        owner_key=key, destination=self.destination_id)
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

    @telemetry.runtime_guard('DCS', 'DCS_SENDER_RUNTIME')
    def send_ready(self, max_jobs: int = 100) -> SendSummary:
        if max_jobs < 1:
            raise ValueError("max_jobs must be positive")
        enqueued = self.enqueue_new()
        sent = 0
        retry_scheduled = 0
        for _ in range(max_jobs):
            job = self._claim_next(_utc_now())
            if job is None:
                break
            try:
                response_code = post_json(
                    self.url,
                    build_payload(job),
                    job["delivery_id"],
                    self.timeout_seconds,
                )
            except DeliveryError as exc:
                self._record_result(
                    job,
                    now=_utc_now(),
                    response_code=exc.response_code,
                    error=str(exc),
                )
                retry_scheduled += 1
            else:
                self._record_result(job, now=_utc_now(), response_code=response_code, error=None)
                sent += 1
        return SendSummary(enqueued, sent, retry_scheduled)
