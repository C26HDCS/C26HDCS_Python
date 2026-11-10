"""Persistent Ka video delivery queue, independent of reception and monitoring."""

import argparse
import json
import hdcs_db as db
import hdcs_telemetry as telemetry
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .transport import DeliveryError, upload_file, validate_url


def timestamp(seconds: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec='milliseconds')


@dataclass(frozen=True)
class SendSummary:
    enqueued: int
    sent: int
    retry_scheduled: int


class KaSender:
    def __init__(self, database: Path, *, destination_id: str, url: str,
                 camera_id: str | None = None, timeout_seconds: int = 30,
                 retry_base_seconds: int = 30, retry_max_seconds: int = 900):
        validate_url(url)
        if not destination_id.strip() or camera_id == '':
            raise ValueError('destination_id and camera filter must not be empty')
        if min(timeout_seconds, retry_base_seconds, retry_max_seconds) < 1:
            raise ValueError('timeout and retry intervals must be positive')
        self.destination_id, self.url, self.camera_id = destination_id, url, camera_id
        self.database = database
        self.timeout_seconds = timeout_seconds
        self.retry_base_seconds, self.retry_max_seconds = retry_base_seconds, retry_max_seconds
        self.lease_seconds = max(60, 2 * timeout_seconds + 30)
        self.connection = db.connect(database, timeout=10)
        self.connection.row_factory = db.Row
        try:
            self.connection.initialize_script('''
                CREATE TABLE IF NOT EXISTS ka_delivery_destinations (
                    destination_id TEXT PRIMARY KEY, url TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ka_delivery_jobs (
                    id INTEGER PRIMARY KEY, receipt_id INTEGER NOT NULL,
                    destination_id TEXT NOT NULL, delivery_id TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT, lease_until TEXT, claim_token TEXT,
                    last_error TEXT, response_code INTEGER, sent_at TEXT,
                    UNIQUE(destination_id, receipt_id)
                );
                CREATE TABLE IF NOT EXISTS ka_delivery_attempts (
                    id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL,
                    attempt_number INTEGER NOT NULL, attempted_at TEXT NOT NULL,
                    finished_at TEXT, outcome TEXT NOT NULL, response_code INTEGER, error TEXT,
                    UNIQUE(job_id, attempt_number)
                );
                CREATE INDEX IF NOT EXISTS idx_ka_delivery_ready
                    ON ka_delivery_jobs(destination_id, status, next_attempt_at);
            ''')
            with self.connection:
                self.connection.execute(
                    'INSERT OR IGNORE INTO ka_delivery_destinations VALUES (?, ?)', (destination_id, url)
                )
                registered = self.connection.execute(
                    'SELECT url FROM ka_delivery_destinations WHERE destination_id = ?', (destination_id,)
                ).fetchone()[0]
                if registered != url:
                    raise ValueError('This destination_id is bound to another URL; use a new destination_id')
        except BaseException:
            self.connection.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.connection.close()

    def enqueue_new(self, batch_size: int = 500) -> int:
        if batch_size < 1:
            raise ValueError('batch_size must be positive')
        if not self.connection.table_exists('ka_video_receipts'):
            return 0
        rows = self.connection.execute('''
            SELECT r.id FROM ka_video_receipts r
            LEFT JOIN ka_delivery_jobs j ON j.receipt_id = r.id AND j.destination_id = ?
            WHERE j.id IS NULL AND (CAST(? AS TEXT) IS NULL OR r.camera_id = ?) ORDER BY r.id LIMIT ?
        ''', (self.destination_id, self.camera_id, self.camera_id, batch_size)).fetchall()
        count = 0
        with self.connection:
            for row in rows:
                count += self.connection.execute('''
                    INSERT OR IGNORE INTO ka_delivery_jobs
                        (receipt_id, destination_id, delivery_id, status, next_attempt_at)
                    VALUES (?, ?, ?, 'PENDING', ?)
                ''', (row['id'], self.destination_id, uuid.uuid4().hex, timestamp())).rowcount
        return count

    def _claim(self) -> dict | None:
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            # The camera filter also applies to jobs queued by an earlier invocation.
            row = self.connection.execute('''
                SELECT j.*, r.camera_id, r.archive_path, r.source_path, r.byte_size, r.sha256, r.received_at
                FROM ka_delivery_jobs j JOIN ka_video_receipts r ON r.id = j.receipt_id
                WHERE j.destination_id = ? AND (CAST(? AS TEXT) IS NULL OR r.camera_id = ?) AND (
                    j.status = 'PENDING' OR (j.status = 'RETRY' AND j.next_attempt_at <= ?)
                    OR (j.status = 'SENDING' AND j.lease_until <= ?)) ORDER BY j.id LIMIT 1
            ''', (self.destination_id, self.camera_id, self.camera_id, timestamp(), timestamp())).fetchone()
            if row is None:
                self.connection.commit()
                return None
            job = dict(row)
            if job['status'] == 'SENDING':
                self.connection.execute('''UPDATE ka_delivery_attempts SET outcome = 'INTERRUPTED',
                    finished_at = ?, error = 'Delivery lease expired'
                    WHERE job_id = ? AND attempt_number = ?''', (timestamp(), job['id'], job['attempts']))
            job['attempts'] += 1
            job['claim_token'] = uuid.uuid4().hex
            self.connection.execute('''UPDATE ka_delivery_jobs SET status = 'SENDING', attempts = ?,
                lease_until = ?, claim_token = ?, next_attempt_at = NULL WHERE id = ?''',
                (job['attempts'], timestamp(self.lease_seconds), job['claim_token'], job['id']))
            self.connection.execute('''INSERT INTO ka_delivery_attempts
                (job_id, attempt_number, attempted_at, outcome) VALUES (?, ?, ?, 'SENDING')''',
                (job['id'], job['attempts'], timestamp()))
            self.connection.commit()
            return job
        except BaseException:
            self.connection.rollback()
            raise

    def _renew(self, job: dict) -> None:
        with self.connection:
            updated = self.connection.execute('''UPDATE ka_delivery_jobs SET lease_until = ?
                WHERE id = ? AND claim_token = ? AND status = 'SENDING' ''',
                (timestamp(self.lease_seconds), job['id'], job['claim_token']))
            if updated.rowcount != 1:
                raise RuntimeError('Delivery ownership was lost')

    def _finish(self, job: dict, code: int | None, error: str | None) -> None:
        status = 'RETRY' if error else 'SENT'
        delay = min(self.retry_max_seconds, self.retry_base_seconds * 2 ** min(job['attempts'] - 1, 30))
        with self.connection:
            updated = self.connection.execute('''UPDATE ka_delivery_jobs SET status = ?,
                next_attempt_at = ?, lease_until = NULL, claim_token = NULL,
                last_error = ?, response_code = ?, sent_at = ?
                WHERE id = ? AND claim_token = ? AND status = 'SENDING' ''',
                (status, timestamp(delay) if error else None, error, code,
                 None if error else timestamp(), job['id'], job['claim_token']))
            if updated.rowcount != 1:
                raise RuntimeError('Delivery ownership was lost')
            self.connection.execute('''UPDATE ka_delivery_attempts SET finished_at = ?, outcome = ?,
                response_code = ?, error = ? WHERE job_id = ? AND attempt_number = ?''',
                (timestamp(), status, code, error, job['id'], job['attempts']))
            device = telemetry.device_for(self.connection, 'KA', job['camera_id'])
            telemetry.record(self.connection, device, 'KA_SEND',
                             f"destination={self.destination_id} job={job['id']} attempt={job['attempts']} status={status} http={code}",
                             'ERROR' if error else 'INFO')
            telemetry.condition(self.connection, device, 'KA_SEND_ERROR', self.destination_id,
                                error is not None, f'destination={self.destination_id} delivery={status} http={code}',
                                category=telemetry.NETWORK, owner_key=job['camera_id'])
            if self.connection.is_postgres:
                from hdcs_status import publish
                publish(self.connection, device, 'KA', 'DELIVERY', str(job['id']),
                        'WARNING' if error else 'NORMAL', f'destination={self.destination_id} status={status}',
                        owner_key=job['camera_id'], destination=self.destination_id)

    @telemetry.runtime_guard('KA', 'KA_SENDER_RUNTIME')
    def send_ready(self, max_jobs: int = 10) -> SendSummary:
        if max_jobs < 1:
            raise ValueError('max_jobs must be positive')
        enqueued = self.enqueue_new()
        if not self.connection.table_exists('ka_video_receipts'):
            return SendSummary(0, 0, 0)
        sent = retries = 0
        for _ in range(max_jobs):
            job = self._claim()
            if job is None:
                break
            last_renewal = time.monotonic()

            def progress():
                nonlocal last_renewal
                if time.monotonic() - last_renewal >= 5:
                    self._renew(job)
                    last_renewal = time.monotonic()

            try:
                code = upload_file(self.url, job, self.timeout_seconds, progress)
            except DeliveryError as exc:
                self._finish(job, exc.response_code, str(exc)[:1000])
                retries += 1
            except KeyboardInterrupt:
                self._finish(job, None, 'Interrupted by operator')
                raise
            else:
                self._finish(job, code, None)
                sent += 1
        return SendSummary(enqueued, sent, retries)


def main():
    parser = argparse.ArgumentParser(description='Forward completed Ka videos using a provisional HTTP API')
    parser.add_argument('--db', type=Path, default=Path(__file__).resolve().parent.parent / 'data' / 'ka_receipts.sqlite3')
    parser.add_argument('--destination-id', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--camera-id')
    parser.add_argument('--timeout-seconds', type=int, default=30)
    parser.add_argument('--retry-base-seconds', type=int, default=30)
    parser.add_argument('--retry-max-seconds', type=int, default=900)
    parser.add_argument('--poll-seconds', type=int, default=5)
    parser.add_argument('--max-jobs', type=int, default=10)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if min(args.poll_seconds, args.max_jobs) < 1:
        parser.error('poll-seconds and max-jobs must be positive')
    try:
        with KaSender(args.db, destination_id=args.destination_id, url=args.url, camera_id=args.camera_id,
                      timeout_seconds=args.timeout_seconds, retry_base_seconds=args.retry_base_seconds,
                      retry_max_seconds=args.retry_max_seconds) as sender:
            first = True
            while True:
                summary = sender.send_ready(args.max_jobs)
                if first or args.once or summary.enqueued or summary.sent or summary.retry_scheduled:
                    print(json.dumps(asdict(summary)), flush=True)
                if args.once:
                    return
                first = False
                time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        pass
    except (ValueError, OSError, db.Error, RuntimeError) as exc:
        from hdcs_status import DeliveryDisabled
        if not isinstance(exc,DeliveryDisabled):
            telemetry.report_failure(args.db, 'KA', 'KA_SENDER_RUNTIME', key=args.camera_id, error=exc)
        parser.exit(1, f'Sender error: {exc}\n')


if __name__ == '__main__':
    main()
