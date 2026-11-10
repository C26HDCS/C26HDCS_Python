"""Database boundary for the existing SQLite trial and PostgreSQL pipeline.

Only the pipeline's fixed SQL identifiers and placeholders are adapted here.
PostgreSQL schema provisioning is explicit: runtime initialization never runs DDL.
"""

import argparse
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import psycopg
import yaml


load_dotenv(Path(__file__).resolve().parent / '.env', override=False)
Row = sqlite3.Row
TABLES = {
    'dcs_frames': 'public.v_python_dcs_frames',
    'ka_video_receipts': 'public.v_python_ka_receipts',
    **{name: 'public.tb_' + name for name in (
        'dcs_monitor_status', 'dcs_monitor_events', 'dcs_delivery_cursors',
        'dcs_delivery_jobs', 'dcs_delivery_attempts', 'ka_receiver_runtime',
        'ka_receiver_pending', 'ka_monitor_status', 'ka_monitor_events',
        'ka_delivery_destinations', 'ka_delivery_jobs', 'ka_delivery_attempts',
    )},
}


class Error(Exception):
    """Configuration, mapping, or database operation failure."""


def postgres_settings() -> dict:
    settings = {}
    config_path = os.getenv('HDCS_WEB_CONFIG')
    if config_path and not all(os.getenv(k) for k in ('PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD')):
        try:
            documents = yaml.safe_load_all(Path(config_path).read_text(encoding='utf-8'))
            datasource = {}
            for document in documents:
                spring = (document or {}).get('spring', {})
                if not spring.get('config', {}).get('activate', {}).get('on-profile'):
                    datasource.update(spring.get('datasource', {}))
            def resolve(value):
                match = re.fullmatch(r'\$\{([^}:]+)(?::(.*))?\}', str(value))
                if not match:
                    return value
                result = os.getenv(match[1], match[2])
                if result is None:
                    raise Error(f'Required configuration environment variable is missing: {match[1]}')
                return result
            url = urlsplit(str(resolve(datasource['url'])).removeprefix('jdbc:'))
            if url.scheme != 'postgresql':
                raise Error('The web datasource must use PostgreSQL')
            settings.update(host=url.hostname, port=url.port or 5432,
                            dbname=url.path.lstrip('/'), user=resolve(datasource['username']),
                            password=resolve(datasource['password']))
            query = parse_qs(url.query)
            if 'sslmode' in query:
                settings['sslmode'] = query['sslmode'][0]
        except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError) as exc:
            raise Error('Cannot read the web datasource configuration; check HDCS_WEB_CONFIG') from exc
    for name, key in [('PGHOST', 'host'), ('PGPORT', 'port'), ('PGDATABASE', 'dbname'),
                      ('PGUSER', 'user'), ('PGPASSWORD', 'password'), ('PGSSLMODE', 'sslmode')]:
        if name in os.environ:
            settings[key] = os.environ[name]
    if not all(settings.get(k) for k in ('host', 'dbname', 'user', 'password')):
        raise Error('PostgreSQL connection configuration is incomplete')
    settings['connect_timeout'] = 5
    settings['application_name'] = 'hdcs-python'
    statement_timeout=int(os.getenv('HDCS_DB_STATEMENT_TIMEOUT_MS','10000'))
    if statement_timeout < 1: raise Error('HDCS_DB_STATEMENT_TIMEOUT_MS must be positive')
    settings['options'] = f'-c timezone=UTC -c statement_timeout={statement_timeout} -c lock_timeout=3000 -c idle_in_transaction_session_timeout=30000'
    return settings


_connect_lock = threading.Lock()
_retry_at = 0.0
_retry_delay = 0


def open_postgres():
    global _retry_at, _retry_delay
    with _connect_lock:
        if time.monotonic() < _retry_at:
            raise Error('PostgreSQL reconnect paused after failure')
        try:
            connection = psycopg.connect(**postgres_settings(), autocommit=True)
        except psycopg.Error as exc:
            _retry_delay = min(60, _retry_delay * 2 if _retry_delay else 1)
            _retry_at = time.monotonic() + _retry_delay
            raise Error(f'PostgreSQL connection failed; retry after {_retry_delay}s') from exc
        _retry_at = 0
        _retry_delay = 0
        return connection


def _value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


class PgRow:
    def __init__(self, names, values):
        self.names = names
        self.values = tuple(_value(v) for v in values)

    def keys(self):
        return self.names

    def __getitem__(self, key):
        return self.values[self.names.index(key)] if isinstance(key, str) else self.values[key]

    def __iter__(self):
        return iter(self.values)


class Cursor:
    def __init__(self, cursor, postgres):
        self.cursor = cursor
        self.postgres = postgres

    @property
    def rowcount(self):
        return self.cursor.rowcount

    @property
    def lastrowid(self):
        return self.cursor.lastrowid

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None or not self.postgres:
            return row
        return PgRow([c.name for c in self.cursor.description], row)

    def fetchall(self):
        return list(self)

    def __iter__(self):
        while (row := self.fetchone()) is not None:
            yield row


def _postgres_sql(sql):
    ignore = bool(re.match(r'\s*INSERT OR IGNORE\b', sql, re.I))
    if ignore:
        sql = re.sub(r'INSERT OR IGNORE', 'INSERT', sql, count=1, flags=re.I)
        sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    # Quoted SQL strings are deliberately left intact.
    tokens = r"'(?:''|[^'])*'|\b[A-Za-z_][A-Za-z_0-9]*\b|\?"
    return re.sub(tokens, lambda m: '%s' if m[0] == '?' else TABLES.get(m[0], m[0]), sql)


class Connection:
    def __init__(self, path: Path, timeout=10):
        backend = os.getenv('HDCS_DB_BACKEND', 'sqlite').lower()
        if backend not in ('sqlite', 'postgresql'):
            raise Error('HDCS_DB_BACKEND must be sqlite or postgresql')
        self.is_postgres = backend == 'postgresql'
        if self.is_postgres:
            self.raw = open_postgres()
            required = list(TABLES.values()) + ['public.tb_python_device_mapping', 'public.tb_dcs_receive_details', 'public.tb_ka_receive_details']
            required += ['public.tb_log', 'public.tb_alarm', 'public.tb_python_alarm_state']
            required += ['public.tb_python_component_status', 'public.v_python_component_health', 'public.tb_python_delivery_target']
            try:
                missing = [name for name, relation in self.raw.execute(
                    'SELECT name, to_regclass(name) FROM unnest(%s::text[]) AS t(name)', (required,)
                ).fetchall() if relation is None]
            except Exception:
                self.raw.close()
                raise
            if missing:
                self.raw.close()
                raise Error('Apply pipeline, telemetry, device status and python_status_lifecycle_postgresql.sql first. Missing: ' + ', '.join(missing))
        else:
            path = Path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.raw = sqlite3.connect(path, timeout=timeout)
            from hdcs_telemetry import initialize
            initialize(self)
            self.raw.commit()

    @property
    def row_factory(self):
        return None if self.is_postgres else self.raw.row_factory

    @row_factory.setter
    def row_factory(self, value):
        if not self.is_postgres:
            self.raw.row_factory = value

    def initialize(self, sql):
        if not self.is_postgres:
            return self.execute(sql)

    def initialize_script(self, sql):
        if not self.is_postgres:
            self.raw.executescript(sql)

    def table_exists(self, name):
        if self.is_postgres:
            return self.raw.execute('SELECT to_regclass(%s)', (TABLES[name],)).fetchone()[0] is not None
        return self.raw.execute('SELECT 1 FROM sqlite_master WHERE name = ?', (name,)).fetchone() is not None

    def execute(self, sql, parameters=()):
        try:
            if self.is_postgres:
                if sql.strip().upper() == 'BEGIN IMMEDIATE':
                    self.raw.execute('BEGIN')
                    # Serialize the short claim/monitor transactions, as in the SQLite implementation.
                    return Cursor(self.raw.execute('SELECT pg_advisory_xact_lock(260014)'), True)
                sql = _postgres_sql(sql)
                writes_or_locks = re.match(r'\s*(INSERT|UPDATE|DELETE)\b', sql, re.I) or re.search(
                    r'\bFOR\s+(?:NO\s+KEY\s+UPDATE|KEY\s+SHARE|UPDATE|SHARE)\b', sql, re.I)
                if writes_or_locks and self.raw.info.transaction_status == psycopg.pq.TransactionStatus.IDLE:
                    self.raw.execute('BEGIN')
                return Cursor(self.raw.execute(sql, parameters), True)
            return Cursor(self.raw.execute(sql, parameters), False)
        except (psycopg.Error, sqlite3.Error) as exc:
            raise Error(str(exc)) from exc

    def commit(self):
        try:
            self.raw.commit()
        except (psycopg.Error, sqlite3.Error) as exc:
            raise Error(str(exc)) from exc

    def rollback(self):
        self.raw.rollback()

    def close(self):
        self.raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def device_id(self, kind, external_key):
        row = self.execute('''SELECT device_id FROM public.tb_python_device_mapping
            WHERE data_kind = ? AND external_key = ? AND enabled = TRUE FOR SHARE''', (kind, external_key)).fetchone()
        return row[0] if row else None


def connect(path, timeout=10):
    return Connection(path, timeout)


def local_timestamp(iso):
    return datetime.fromisoformat(iso).astimezone(ZoneInfo(os.getenv('HDCS_LOCAL_TIMEZONE', 'Asia/Seoul'))).replace(tzinfo=None)


def save_dcs(connection, values):
    """Preserve unmapped/invalid frames without inventing a device association."""
    (received, source, raw_hex, status, error, station, system_group, group, *_) = values
    key = f'{system_group}/{group}/{station}' if status == 'PARSED' else None
    device = connection.device_id('DCS', key) if key else None
    parent_id = None
    if device is not None:
        parent_id = connection.execute('''INSERT INTO public.tb_dcs_data
            (device_id, data_payload, received_status, received_at) VALUES (?, ?, ?, ?)
            RETURNING dcs_data_id''', (device, raw_hex, 'SUCCESS', local_timestamp(received))).fetchone()[0]
    mapping_error = None if parent_id is not None else ('Unmapped DCS key: ' + key if key else 'Frame cannot identify a device')
    columns = ('received_at,source,raw_hex,parse_status,parse_error,station_number,system_group_number,'
               'group_number,field_8_9,voltage,parity_1,field_13,quantity_1,parity_2,field_21,quantity_2,'
               'quantity_3,quantity_4,error_code_hex,document_xor_hex,document_xor_matches')
    params = list(values)
    params[-1] = bool(params[-1]) if params[-1] is not None else None
    row = connection.execute(f'''INSERT INTO public.tb_dcs_receive_details
        ({columns}, dcs_data_id, mapping_error) VALUES ({','.join('?' for _ in range(23))}) RETURNING id''',
        (*params, parent_id, mapping_error)).fetchone()
    from hdcs_telemetry import record, condition, device_for, DATA
    log_device = device if device is not None else device_for(connection, 'DCS')
    if parent_id is not None:
        record(connection, device, 'DCS_RECEIVE', f'record={parent_id} received and parsed')
        from hdcs_status import receipt
        receipt(connection, device, received)
    condition(connection, log_device, 'DCS_RECEIVE_INVALID', key or 'receiver',
              parent_id is None, error or mapping_error or 'Valid frame received', category=DATA)
    if parent_id is not None:
        condition(connection, device_for(connection, 'DCS'), 'DCS_RECEIVE_INVALID', 'receiver',
                  False, 'Valid frame received', category=DATA)
    connection.commit()
    return row[0], mapping_error


def save_ka(connection, values):
    received, camera, source, marker, mtime, marker_size, archive, size, digest = values
    device = connection.device_id('KA', camera)
    if device is None:
        raise Error(f'No enabled KA device mapping for {camera}; register it using python -m hdcs_db map-device')
    parent_id = connection.execute('''INSERT INTO public.tb_ka_data
        (device_id, file_path, received_at, file_size, file_name, receive_status)
        VALUES (?, ?, ?, ?, ?, 'SUCCESS') RETURNING ka_data_id''',
        (device, archive, local_timestamp(received), size, Path(source).name)).fetchone()[0]
    connection.execute('''INSERT INTO public.tb_ka_receive_details
        (ka_data_id, camera_id, source_path, marker_path, marker_mtime_ns, marker_size, sha256)
        VALUES (?, ?, ?, ?, ?, ?, ?)''', (parent_id, camera, source, marker, mtime, marker_size, digest))
    from hdcs_telemetry import record
    record(connection, device, 'KA_RECEIVE', f'record={parent_id} camera={camera} bytes={size} received')
    from hdcs_status import receipt
    receipt(connection, device, received)
    connection.commit()
    return parent_id


def main():
    parser = argparse.ArgumentParser(description='Check PostgreSQL setup or register an explicit device mapping')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('check')
    mapping = commands.add_parser('map-device')
    mapping.add_argument('--kind', required=True, choices=['DCS', 'KA'])
    mapping.add_argument('--external-key', required=True)
    mapping.add_argument('--device-id', required=True, type=int)
    disable = commands.add_parser('disable-mapping')
    disable.add_argument('--kind', required=True, choices=['DCS','KA'])
    disable.add_argument('--external-key', required=True)
    destination = commands.add_parser('destination-state')
    destination.add_argument('--kind', required=True, choices=['DCS','KA'])
    destination.add_argument('--destination-id', required=True)
    destination.add_argument('--enabled', required=True, choices=['true','false'])
    args = parser.parse_args()
    try:
        with open_postgres() as connection:
            if args.command == 'check':
                found = {name: connection.execute('SELECT to_regclass(%s) IS NOT NULL', (name,)).fetchone()[0]
                         for name in list(TABLES.values()) + ['public.tb_python_device_mapping',
                             'public.tb_log', 'public.tb_alarm', 'public.tb_python_alarm_state',
                             'public.tb_python_component_status', 'public.v_python_component_health', 'public.tb_python_delivery_target']}
                print(json.dumps({'connected': True, 'schema_ready': all(found.values()),
                                  'missing': [name for name, ok in found.items() if not ok]}, ensure_ascii=False))
            elif args.command == 'disable-mapping':
                connection.execute('''UPDATE public.tb_python_device_mapping SET enabled=FALSE
                    WHERE data_kind=%s AND external_key=%s''',(args.kind,args.external_key))
                print('Mapping disabled; runtime state retired')
            elif args.command == 'destination-state':
                connection.execute('''INSERT INTO public.tb_python_delivery_target(data_kind,destination_id,enabled)
                    VALUES(%s,%s,%s) ON CONFLICT(data_kind,destination_id) DO UPDATE SET enabled=excluded.enabled''',
                    (args.kind,args.destination_id,args.enabled=='true'))
                print('Destination state updated')
            else:
                if args.device_id < 1 or not args.external_key.strip():
                    parser.error('A positive device-id and nonempty external-key are required')
                connection.execute('''INSERT INTO public.tb_python_device_mapping
                    (data_kind, external_key, device_id) VALUES (%s, %s, %s)
                    ON CONFLICT (data_kind, external_key) DO UPDATE
                    SET device_id = excluded.device_id, enabled = TRUE''',
                    (args.kind, args.external_key, args.device_id))
                connection.execute('''INSERT INTO public.tb_python_component_status
                    (device_id,data_kind,component,scope,status,detail,checked_at,expires_at)
                    VALUES (%s,%s,'MONITOR',%s,'WARNING','Waiting for first monitor check',
                            CURRENT_TIMESTAMP,CURRENT_TIMESTAMP + INTERVAL '120 seconds')
                    ON CONFLICT DO NOTHING''', (args.device_id, args.kind, args.external_key))
                print('Device mapping registered')
    except (Error, psycopg.Error) as exc:
        parser.exit(1, f'Database error: {exc}\n')


if __name__ == '__main__':
    main()
