"""Shared device logs and incident alarms, committed with pipeline state."""

import hashlib
from functools import wraps
import logging
from hdcs_log_policy import CalendarLogHandler, start_heartbeat
import os
from pathlib import Path
import re
import threading
from datetime import datetime, timezone


_lock = threading.Lock()
_logger = None
_heartbeat = None
SYSTEM = "\uC2DC\uC2A4\uD15C"
NETWORK = "\uB124\uD2B8\uC6CC\uD06C"
DATA = "\uB370\uC774\uD130"

STATUS_LABELS = {
    'RUNNING': '동작 중', 'STOPPED': '정지', 'NEVER_STARTED': '시작 이력 없음',
    'UNRESPONSIVE': '응답 없음', 'ERROR': '오류', 'RECEIVING': '수신 중',
    'NO_DATA': '수신 이력 없음', 'STALE': '최근 수신 없음',
    'INVALID_ONLY': '유효한 자료 없음', 'NONE': '대기 없음',
    'WAITING': '대기 중', 'DELAYED': '처리 지연',
}
INCIDENT_LABELS = {
    'DCS_RECEIVE_INVALID': ('유효하지 않은 수문 자료가 수신되었습니다.', '유효한 수문 자료가 다시 수신되고 있습니다.'),
    'KA_RECEIVE_ERROR': ('영상 수신 중 오류가 발생했습니다.', '영상 수신 오류가 해소되었습니다.'),
    'DCS_SEND_ERROR': ('수문 자료 전송에 실패했습니다.', '수문 자료 전송이 정상화되었습니다.'),
    'KA_SEND_ERROR': ('영상 전송에 실패했습니다.', '영상 전송이 정상화되었습니다.'),
    'KA_PENDING_DELAY': ('수신 대기 중인 영상의 처리가 지연되고 있습니다.', '영상 처리 지연이 해소되었습니다.'),
}
RUNTIME_LABELS = {
    'RECEIVER': '수신기', 'SENDER': '전송기', 'MONITOR': '감시기',
}


def status_label(detail, field):
    matches = re.findall(rf'(?:^| ){field}=([A-Z_]+)(?= |$)', detail)
    return STATUS_LABELS.get(matches[-1] if matches else '', '확인 필요')


def operator_message(code, detail, level, active=None):
    subject = '영상' if code.startswith('KA_') else '수문 자료'
    if code == 'KA_MONITOR':
        message = (f'영상 수신기 {status_label(detail, "receiver")} / '
                   f'영상 {status_label(detail, "data")} / '
                   f'처리 대기 상태: {status_label(detail, "pending")}')
    elif code in ('DCS_MONITOR', 'DCS_DATA_STATUS', 'KA_DATA_STATUS'):
        field = 'data' if code == 'KA_DATA_STATUS' else 'status'
        message = f'{subject} 수신 상태: {status_label(detail, field)}'
    elif code == 'KA_RECEIVER_STATUS':
        message = f'영상 수신기 상태: {status_label(detail, "receiver")}'
    elif code.endswith('_RUNTIME'):
        component = RUNTIME_LABELS.get(code.split('_')[-2], '처리 프로그램')
        message = (f'{subject} {component}에 오류가 발생했습니다.' if active is not False
                   else f'{subject} {component} 동작이 정상화되었습니다.')
    elif code in INCIDENT_LABELS:
        fault, recovery = INCIDENT_LABELS[code]
        message = recovery if active is False else fault
    elif code.endswith('_RECEIVER'):
        action = '정지했습니다' if detail.endswith(' stopped') else '동작을 시작했습니다'
        message = f'{subject} 수신기가 {action}.'
    elif code.endswith('_RECEIVE'):
        target = '영상을' if code == 'KA_RECEIVE' else '수문 자료를'
        message = (f'{target} 수신했습니다.' if level == 'INFO'
                   else f'수신한 {target} 확인해 주세요.')
    elif code.endswith('_SEND'):
        message = (f'{subject} 전송을 완료했습니다.' if level == 'INFO'
                   else f'{subject} 전송에 실패했습니다. 재시도할 예정입니다.')
    else:
        message = ('장치 동작 중 오류가 발생했습니다.' if level == 'ERROR'
                   else '장치 상태를 확인해 주세요.')
    return f'[{code}] {message}'


def logger():
    global _logger, _heartbeat
    with _lock:
        if _logger is None:
            target = Path(os.getenv('HDCS_LOG_DIR', str(Path(__file__).resolve().parent / 'logs')))
            log = logging.getLogger('hdcs.pipeline')
            log.setLevel(logging.INFO)
            log.propagate = False
            try:
                target.mkdir(parents=True, exist_ok=True)
                handler = CalendarLogHandler(target)
            except OSError:
                handler = logging.StreamHandler()
                handler.stream.write('Python file logging unavailable; using stderr\n')
            if not isinstance(handler, CalendarLogHandler):
                handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s',
                                                      datefmt='%Y-%m-%d %H:%M:%S'))
            log.addHandler(handler)
            _logger = log
            if isinstance(handler, CalendarLogHandler):
                _heartbeat = start_heartbeat(log, handler)
            else:
                log.error('One-second file persistence unavailable; inspect log directory')
        return _logger


def text(value, limit):
    # One-line records; never include raw frames, URLs with credentials or videos.
    return str(value).replace('\x00', '').replace('\r', ' ').replace('\n', ' ')[:limit]


def initialize(connection):
    connection.initialize_script('''
        CREATE TABLE IF NOT EXISTS tb_log (
            id INTEGER PRIMARY KEY, device_id INTEGER NOT NULL, level VARCHAR(10) NOT NULL,
            message VARCHAR(1000) NOT NULL, reg_dt TIMESTAMP NOT NULL);
        CREATE TABLE IF NOT EXISTS tb_alarm (
            alarm_id INTEGER PRIMARY KEY, device_id INTEGER NOT NULL, alarm_type VARCHAR(50) NOT NULL,
            alarm_level VARCHAR(20) NOT NULL, alarm_message VARCHAR(500) NOT NULL,
            occurred_at TIMESTAMP NOT NULL);
        CREATE TABLE IF NOT EXISTS tb_python_alarm_state (
            device_id INTEGER NOT NULL, incident_key VARCHAR(64) NOT NULL,
            active BOOLEAN NOT NULL DEFAULT FALSE, updated_at TIMESTAMP NOT NULL,
            PRIMARY KEY(device_id, incident_key));
    ''')


def table(connection, name):
    return ('public.' if connection.is_postgres else '') + name


def now():
    # Existing WebService tables use timestamp without time zone.
    from hdcs_db import local_timestamp
    return local_timestamp(datetime.now(timezone.utc).isoformat())


def device_for(connection, kind, key=None):
    if connection.is_postgres and key:
        device = connection.device_id(kind, key)
        if device is not None:
            return device
    value = os.getenv(f'HDCS_{kind}_DEVICE_ID')
    if value:
        device = int(value)
        if device <= 0:
            raise ValueError(f'HDCS_{kind}_DEVICE_ID must be positive')
        return device
    if connection.is_postgres and key is None:
        devices = connection.execute('''SELECT DISTINCT device_id FROM public.tb_python_device_mapping
            WHERE data_kind = ? AND enabled = TRUE LIMIT 2''', (kind,)).fetchall()
        if len(devices) == 1:
            return devices[0][0]
    return None


def timestamp(connection):
    value = now()
    return value if connection.is_postgres else value.isoformat(sep=' ')


def record(connection, device, operation, message, level='INFO', *, display=None):
    if level not in ('DEBUG', 'INFO', 'WARN', 'ERROR'):
        raise ValueError('Unsupported log level')
    logger().log(getattr(logging, 'WARNING' if level == 'WARN' else level),
                 'device=%s %s', device, text(f'[Python:{operation}] {message}', 1000))
    if device is None:
        # Do not invent an FK association when a frame cannot identify a device.
        logger().warning('DB log/alarm unavailable: configure device mapping or HDCS_<band>_DEVICE_ID')
        return
    display = operator_message(operation, message, level) if display is None else display
    connection.execute(f'''INSERT INTO {table(connection, 'tb_log')}
        (device_id, level, message, reg_dt) VALUES (?, ?, ?, ?)''',
        (device, level, text(display, 1000), timestamp(connection)))


def condition(connection, device, code, scope, active, message, *,
              category=SYSTEM, severity='HIGH', owner_key=None):
    """One alarm per fault episode, serialized across processes and restarts."""
    if device is None:
        if active:
            record(connection, None, code, message, 'ERROR')
        return False
    if connection.is_postgres:
        from hdcs_status import publish
        kind = 'KA' if code.startswith('KA_') else 'DCS'
        warning = code in ('DCS_DATA_STATUS', 'KA_DATA_STATUS', 'KA_PENDING_DELAY', 'DCS_SEND_ERROR', 'KA_SEND_ERROR')
        owned = publish(connection, device, kind, code, scope,
                ('WARNING' if warning else 'ERROR') if active else 'NORMAL', message,
                owner_key=owner_key, incident_key=hashlib.sha256(f'{code}:{scope}'.encode('utf-8')).hexdigest(),
                destination=scope if code in ('DCS_SEND_ERROR','KA_SEND_ERROR') else None)
        if not owned: return False
    state = table(connection, 'tb_python_alarm_state')
    key = hashlib.sha256(f'{code}:{scope}'.encode('utf-8')).hexdigest()
    connection.execute(f'''INSERT INTO {state} (device_id, incident_key, active, updated_at)
        VALUES (?, ?, FALSE, ?) ON CONFLICT(device_id, incident_key) DO NOTHING''',
        (device, key, timestamp(connection)))
    suffix = ' FOR UPDATE' if connection.is_postgres else ''
    previous = connection.execute(f'''SELECT active FROM {state}
        WHERE device_id = ? AND incident_key = ?{suffix}''', (device, key)).fetchone()[0]
    if bool(previous) == bool(active):
        return False
    level = 'ERROR' if active else 'INFO'
    display = operator_message(code, message, level, active)
    record(connection, device, code, ('FAULT: ' if active else 'RECOVERED: ') + message,
           level, display=display)
    if active:
        connection.execute(f'''INSERT INTO {table(connection, 'tb_alarm')}
            (device_id, alarm_type, alarm_level, alarm_message, occurred_at)
            VALUES (?, ?, ?, ?, ?)''',
            (device, category, severity, text(display, 500), timestamp(connection)))
    connection.execute(f'''UPDATE {state} SET active = ?, updated_at = ?
        WHERE device_id = ? AND incident_key = ?''', (bool(active), timestamp(connection), device, key))
    return True


def report_failure(database, kind, operation, *, key=None, error=None, active=True):
    """Best effort crash alarm on a fresh connection; preserve the original error."""
    import hdcs_db as db
    from contextlib import closing
    # Exception strings may contain connection secrets; persist only the class name.
    detail = type(error).__name__ if error is not None else 'Unexpected failure'
    if active:
        logger().error('%s failed: %s (inspect stderr for details)', operation, detail)
    try:
        with closing(db.connect(database)) as connection:
            with connection:
                condition(connection, device_for(connection, kind, key),
                          operation, key or kind, active, detail if active else 'Operation resumed')
    except Exception:
        logger().error('Could not persist crash alarm; DB may be unavailable')


def runtime_guard(kind, operation):
    def decorate(method):
        @wraps(method)
        def wrapped(self, *args, **kwargs):
            import hdcs_db as db
            logger()
            key = getattr(self, 'camera_id', None) or getattr(self, 'station_key', None)
            if os.getenv('HDCS_DB_BACKEND', 'sqlite').lower() == 'postgresql' and getattr(self, 'destination_id', None):
                from contextlib import closing
                from hdcs_status import ensure_destination
                with closing(db.connect(self.database)) as connection, connection:
                    ensure_destination(connection, kind, self.destination_id)
            try:
                result = method(self, *args, **kwargs)
            except (db.Error, OSError, RuntimeError) as exc:
                report_failure(self.database, kind, operation, key=key, error=exc)
                setattr(self, '_healthy_' + operation, False)
                raise
            if not getattr(self, '_healthy_' + operation, False):
                report_failure(self.database, kind, operation, key=key, active=False)
                setattr(self, '_healthy_' + operation, True)
            if os.getenv('HDCS_DB_BACKEND', 'sqlite').lower() == 'postgresql':
                from contextlib import closing
                from hdcs_status import runtime
                with closing(db.connect(self.database)) as connection:
                    with connection:
                        runtime(connection, kind, operation, key, getattr(self, 'destination_id', None), True)
            return result
        return wrapped
    return decorate
