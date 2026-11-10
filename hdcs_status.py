"""PostgreSQL device projection, with independent component ownership."""

import hashlib
import os
from datetime import datetime, timedelta, timezone

class DeliveryDisabled(ValueError):
    pass


def ttl_seconds():
    value = int(os.getenv('HDCS_STATUS_TTL_SECONDS', '120'))
    if value < 10:
        raise ValueError('HDCS_STATUS_TTL_SECONDS must be at least 10')
    return value


def publish(connection, device, kind, component, scope, status, detail, *, ttl=None, owner_key=None,
            destination=None, incident_key=None):
    if not connection.is_postgres or device is None:
        return
    if status not in ('NORMAL', 'WARNING', 'ERROR') or kind not in ('DCS', 'KA'):
        raise ValueError('Invalid device status')
    scope = str(scope or '')
    if len(scope) > 255:
        scope = hashlib.sha256(scope.encode()).hexdigest()
    now = datetime.now(timezone.utc)
    if owner_key is None:
        rows = connection.execute('''SELECT external_key FROM public.tb_python_device_mapping
            WHERE device_id=? AND data_kind=? AND enabled=TRUE''', (device, kind)).fetchall()
        exact = [r[0] for r in rows if r[0] == scope]
        owner_key = exact[0] if exact else rows[0][0] if len(rows) == 1 else None
    if destination:
        ensure_destination(connection, kind, destination)
    mapped = connection.execute('''SELECT 1 FROM public.tb_python_device_mapping
        WHERE device_id=? AND data_kind=? AND enabled=TRUE AND (CAST(? AS VARCHAR) IS NULL OR external_key=?) LIMIT 1 FOR SHARE''',
        (device, kind, owner_key, owner_key)).fetchone()
    if not mapped:
        return False
    # Lock the device before any component write; all writers use this order.
    connection.execute('SELECT device_id FROM public.tb_device WHERE device_id = ? FOR NO KEY UPDATE', (device,))
    if component == 'DELIVERY' and status == 'NORMAL':
        # Completed jobs already have delivery history; only unresolved jobs need live state.
        connection.execute('''DELETE FROM public.tb_python_component_status
            WHERE device_id=? AND data_kind=? AND component=? AND scope=?''', (device, kind, component, scope))
        connection.execute('SELECT public.hdcs_refresh_python_device_status(?)', (device,))
        return True
    connection.execute('''INSERT INTO public.tb_python_component_status
        (device_id,data_kind,component,scope,status,detail,checked_at,expires_at,owner_key,destination_id,incident_key)
        VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(device_id,data_kind,component,scope) DO UPDATE
        SET status=excluded.status,detail=excluded.detail,checked_at=excluded.checked_at,expires_at=excluded.expires_at,
            owner_key=excluded.owner_key,destination_id=excluded.destination_id,incident_key=excluded.incident_key''',
        (device, kind, component, scope, status, str(detail).replace('\x00', '')[:500], now,
         now + timedelta(seconds=ttl) if ttl else None, owner_key, destination, incident_key))
    connection.execute('SELECT public.hdcs_refresh_python_device_status(?)', (device,))
    return True


def receipt(connection, device, received_at, status='SUCCESS'):
    if not connection.is_postgres or device is None:
        return
    from hdcs_db import local_timestamp
    connection.execute('''UPDATE public.tb_device SET last_received_at=?, last_receive_status=?
        WHERE device_id=? AND (last_received_at IS NULL OR last_received_at <= ?)''',
        (local_timestamp(received_at), status, device, local_timestamp(received_at)))


def runtime(connection, kind, operation, key, destination, healthy):
    if not connection.is_postgres:
        return
    if key:
        rows = connection.execute('''SELECT device_id FROM public.tb_python_device_mapping
            WHERE data_kind=? AND external_key=? AND enabled=TRUE ORDER BY device_id FOR SHARE''', (kind, key)).fetchall()
    else:
        rows = connection.execute('''SELECT device_id FROM public.tb_python_device_mapping
            WHERE data_kind=? AND enabled=TRUE ORDER BY device_id,external_key FOR SHARE''', (kind,)).fetchall()
    for device in sorted({row[0] for row in rows}):
        publish(connection, device, kind, operation, (key or '*') + '|' + (destination or ''),
                'NORMAL' if healthy else 'ERROR', 'Operation responsive' if healthy else 'Operation failed',
                ttl=ttl_seconds(), owner_key=key, destination=destination)


def ensure_destination(connection, kind, destination):
    if not connection.is_postgres or not destination: return
    connection.execute('''INSERT INTO public.tb_python_delivery_target(data_kind,destination_id)
        VALUES(?,?) ON CONFLICT DO NOTHING''', (kind,destination))
    if not connection.execute('''SELECT enabled FROM public.tb_python_delivery_target
        WHERE data_kind=? AND destination_id=?''', (kind,destination)).fetchone()[0]:
        raise DeliveryDisabled('Delivery destination is disabled')


def monitor_scope(connection, device, kind, key):
    if key or not connection.is_postgres or device is None:
        return key
    rows = connection.execute('''SELECT external_key FROM public.tb_python_device_mapping
        WHERE data_kind=? AND device_id=? AND enabled=TRUE''', (kind, device)).fetchall()
    return rows[0][0] if len(rows) == 1 else None
