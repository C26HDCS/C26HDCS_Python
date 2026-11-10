import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from contextlib import closing
from datetime import datetime, timedelta, timezone

import hdcs_db as db
import hdcs_telemetry as telemetry
from dcs_receiver.__main__ import SAMPLE_HEX
from dcs_receiver.receiver import Ingestor
from dcs_receiver.storage import FrameStore
from dcs_receiver.protocol import parse_frame
from dcs_receiver.monitor import DcsMonitor
from dcs_receiver.sender import DcsSender, DeliveryError
from ka_receiver.receiver import KaReceiver
from ka_receiver.monitor import KaMonitor
from ka_receiver.sender import KaSender
from ka_receiver.transport import DeliveryError as KaDeliveryError


class TelemetryTests(unittest.TestCase):
    def setUp(self):
        self.mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite',
                                           'HDCS_DCS_DEVICE_ID': '75', 'HDCS_KA_DEVICE_ID': '86'})
        self.mode.start()
        self.addCleanup(self.mode.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / 'pipeline.sqlite3'

    def count(self, table):
        with closing(db.connect(self.path)) as connection:
            return connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]

    def test_incident_restart_recovery_recurrence_and_operator_messages(self):
        for _ in range(2):
            with closing(db.connect(self.path)) as connection, connection:
                telemetry.condition(connection, 75, 'DCS_SEND_ERROR', 'receiver', True, 'x' * 1500)
        self.assertEqual(self.count('tb_alarm'), 1)
        with closing(db.connect(self.path)) as connection, connection:
            row = connection.execute('SELECT alarm_type, alarm_level, alarm_message FROM tb_alarm').fetchone()
            self.assertEqual(tuple(row), (telemetry.SYSTEM, 'HIGH', '[DCS_SEND_ERROR] 수문 자료 전송에 실패했습니다.'))
            telemetry.condition(connection, 75, 'DCS_SEND_ERROR', 'receiver', False, 'healthy')
            recovery = connection.execute('SELECT message FROM tb_log ORDER BY id DESC LIMIT 1').fetchone()
            self.assertEqual(recovery[0], '[DCS_SEND_ERROR] 수문 자료 전송이 정상화되었습니다.')
            telemetry.condition(connection, 75, 'DCS_SEND_ERROR', 'receiver', True, 'fault again')
        self.assertEqual(self.count('tb_alarm'), 2)
        self.assertEqual(self.count('tb_log'), 3)

    def test_monitor_diagnostics_stay_in_file_logger(self):
        detail = 'camera=cam_test receiver=RUNNING data=RECEIVING pending=NONE'
        with patch.object(telemetry, 'logger') as logger:
            with closing(db.connect(self.path)) as connection, connection:
                telemetry.record(connection, 86, 'KA_MONITOR', detail)
                row = connection.execute('SELECT message FROM tb_log').fetchone()
        self.assertEqual(logger.return_value.log.call_args.args[-1], '[Python:KA_MONITOR] ' + detail)
        self.assertEqual(row[0], '[KA_MONITOR] 영상 수신기 동작 중 / 영상 수신 중 / 처리 대기 상태: 대기 없음')

    def test_alarm_and_log_roll_back_together(self):
        with closing(db.connect(self.path)) as connection:
            try:
                with connection:
                    telemetry.condition(connection, 75, 'TEST', 'rollback', True, 'fault')
                    raise RuntimeError('rollback')
            except RuntimeError:
                pass
        self.assertEqual(self.count('tb_log'), 0)
        self.assertEqual(self.count('tb_alarm'), 0)
        self.assertEqual(self.count('tb_python_alarm_state'), 0)

    def test_parameter_binding_and_missing_device(self):
        with closing(db.connect(self.path)) as connection, connection:
            telemetry.record(connection, 75, 'TEST', "'; DROP TABLE tb_log; --\nline\x00")
            telemetry.record(connection, None, 'TEST', 'no device')
            telemetry.condition(connection, None, 'TEST', 'none', True, 'no device')
        self.assertEqual(self.count('tb_log'), 1)
        self.assertEqual(self.count('tb_alarm'), 0)

    def test_concurrent_detectors_emit_one_alarm(self):
        with closing(db.connect(self.path)):
            pass
        errors = []
        def detect():
            try:
                with closing(db.connect(self.path)) as connection, connection:
                    telemetry.condition(connection, 75, 'TEST', 'concurrent', True, 'fault')
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=detect) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(self.count('tb_alarm'), 1)

    def test_dcs_receive_invalid_monitor_station_isolation_and_recovery(self):
        with FrameStore(self.path) as store:
            Ingestor(store, 'tcp:test').feed(bytes.fromhex(SAMPLE_HEX))
        monitor = DcsMonitor(self.path, station_key='44/56/01', stale_after_seconds=1)
        self.assertEqual(monitor.check().snapshot.status, 'RECEIVING')
        monitor.check(datetime.now(timezone.utc) + timedelta(seconds=5))
        count = self.count('tb_alarm')
        monitor.check(datetime.now(timezone.utc) + timedelta(seconds=6))
        self.assertEqual(self.count('tb_alarm'), count)
        other = DcsMonitor(self.path, station_key='44/56/99')
        self.assertEqual(other.check().snapshot.status, 'NO_DATA')
        with FrameStore(self.path) as store:
            Ingestor(store, 'tcp:test').feed(b'\x0200\r')
        self.assertGreater(self.count('tb_alarm'), count)
        with FrameStore(self.path) as store:
            Ingestor(store, 'tcp:test').feed(bytes.fromhex(SAMPLE_HEX))
        self.assertEqual(monitor.check().snapshot.status, 'RECEIVING')
        self.assertGreater(self.count('tb_log'), 3)

    def test_ka_receiver_error_is_immediate_without_monitor_and_deduplicated(self):
        incoming = self.root / 'incoming'
        incoming.mkdir()
        (incoming / 'clip.dump').touch()
        (incoming / 'clip.mp4').write_bytes(b'one')
        (incoming / 'clip.mkv').write_bytes(b'two')
        for _ in range(2):
            with KaReceiver(incoming, self.root / 'archive', self.path, 'cam01', settle_seconds=0) as receiver:
                self.assertEqual(receiver.scan_once()[0].status, 'ERROR')
                self.assertEqual(receiver.scan_once()[0].status, 'ERROR')
        self.assertEqual(self.count('tb_alarm'), 1)
        (incoming / 'clip.mkv').unlink()
        with KaReceiver(incoming, self.root / 'archive', self.path, 'cam01', settle_seconds=0) as receiver:
            self.assertEqual(receiver.scan_once()[0].status, 'RECEIVED')
        self.assertEqual(self.count('tb_alarm'), 1)

    def test_ka_monitor_detects_process_loss_and_pending_delay(self):
        incoming = self.root / 'incoming'
        incoming.mkdir()
        (incoming / 'clip.mp4').write_bytes(b'video')
        with KaReceiver(incoming, self.root / 'archive', self.path, 'cam01', settle_seconds=0) as receiver:
            receiver.scan_once()
            monitor = KaMonitor(self.path, 'cam01')
            monitor.check()
            before = self.count('tb_alarm')
            monitor.check(datetime.now(timezone.utc) + timedelta(seconds=301))
            self.assertEqual(self.count('tb_alarm'), before + 2)
            monitor.check(datetime.now(timezone.utc) + timedelta(seconds=302))
            self.assertEqual(self.count('tb_alarm'), before + 2)

    def test_dcs_failed_send_alarm_recovers_after_success(self):
        with FrameStore(self.path) as store:
            Ingestor(store, 'tcp:test').feed(bytes.fromhex(SAMPLE_HEX))
        with DcsSender(self.path, destination_id='test', url='http://127.0.0.1:1/dcs') as sender:
            with patch('dcs_receiver.sender.post_json', side_effect=DeliveryError('failure', 503)):
                self.assertEqual(sender.send_ready().retry_scheduled, 1)
            self.assertEqual(self.count('tb_alarm'), 1)
            sender._connection.execute("UPDATE dcs_delivery_jobs SET next_attempt_at='2000-01-01'")
            sender._connection.commit()
            with patch('dcs_receiver.sender.post_json', return_value=200):
                self.assertEqual(sender.send_ready().sent, 1)
        with closing(db.connect(self.path)) as connection:
            self.assertEqual(connection.execute('SELECT active FROM tb_python_alarm_state WHERE active=TRUE').fetchall(), [])

    def test_ka_failed_send_records_attempt_and_alarm(self):
        incoming = self.root / 'incoming'
        incoming.mkdir()
        (incoming / 'clip.mp4').write_bytes(b'video')
        (incoming / 'clip.dump').touch()
        with KaReceiver(incoming, self.root / 'archive', self.path, 'cam01', settle_seconds=0) as receiver:
            receiver.scan_once()
        with KaSender(self.path, destination_id='test', url='http://127.0.0.1:1/ka') as sender:
            with patch('ka_receiver.sender.upload_file', side_effect=KaDeliveryError('failure', 503)):
                self.assertEqual(sender.send_ready().retry_scheduled, 1)
        self.assertEqual(self.count('tb_alarm'), 1)
        with closing(db.connect(self.path)) as connection:
            self.assertEqual(connection.execute('SELECT outcome FROM ka_delivery_attempts').fetchone()[0], 'RETRY')

    def test_runtime_failure_preserves_original_error_even_if_alarm_db_is_down(self):
        monitor = DcsMonitor(self.path)
        with patch('dcs_receiver.monitor.db.connect', side_effect=db.Error('DB unavailable')):
            with self.assertRaisesRegex(db.Error, 'DB unavailable'):
                monitor.check()


@unittest.skipUnless(os.getenv('HDCS_TEST_TELEMETRY_POSTGRES') == '1',
                     'Requires disposable localhost hdcs_test_* database')
class PostgresTelemetryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings = db.postgres_settings()
        if settings['host'] not in ('localhost', '127.0.0.1') or not settings['dbname'].startswith('hdcs_test_'):
            raise RuntimeError('Only disposable localhost hdcs_test_* databases are allowed')
        cls.sql = Path(os.environ['HDCS_TELEMETRY_SQL']).read_text(encoding='utf-8')
        with db.open_postgres() as connection:
            connection.execute('''
                CREATE TABLE IF NOT EXISTS public.tb_device (device_id BIGINT PRIMARY KEY);
                INSERT INTO public.tb_device VALUES(75) ON CONFLICT DO NOTHING;
                CREATE TABLE IF NOT EXISTS public.tb_log (
                    id BIGSERIAL PRIMARY KEY, device_id BIGINT NOT NULL REFERENCES public.tb_device,
                    level VARCHAR(10) NOT NULL, message VARCHAR(1000) NOT NULL, reg_dt TIMESTAMP NOT NULL);
                CREATE TABLE IF NOT EXISTS public.tb_alarm (
                    alarm_id BIGSERIAL PRIMARY KEY, device_id BIGINT NOT NULL REFERENCES public.tb_device,
                    alarm_type VARCHAR(50) NOT NULL, alarm_level VARCHAR(20),
                    alarm_message VARCHAR(500), occurred_at TIMESTAMP NOT NULL);
            ''')
            connection.execute(cls.sql)
            connection.execute(cls.sql)
            from status_fixture import provision
            provision(connection)
            connection.execute("INSERT INTO public.tb_python_device_mapping VALUES('DCS','44/56/01',75,TRUE) ON CONFLICT DO NOTHING")

    def connect(self):
        # Exercise the real SQL adapter without requiring unrelated pipeline tables.
        connection = db.Connection.__new__(db.Connection)
        connection.is_postgres = True
        connection.raw = db.open_postgres()
        return connection

    def setUp(self):
        with closing(self.connect()) as connection, connection:
            connection.execute('DELETE FROM public.tb_python_component_status')
            connection.execute('DELETE FROM public.tb_python_alarm_state')
            connection.execute('DELETE FROM public.tb_alarm')
            connection.execute('DELETE FROM public.tb_log')

    def test_postgres_concurrent_dedup_recovery_and_rollback(self):
        errors = []
        def detect():
            try:
                with closing(self.connect()) as connection, connection:
                    telemetry.condition(connection, 75, 'TEST', 'concurrent', True, 'fault')
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=detect) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        with closing(self.connect()) as connection, connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM public.tb_alarm').fetchone()[0], 1)
            telemetry.condition(connection, 75, 'TEST', 'concurrent', False, 'healthy')
        with closing(self.connect()) as connection, connection:
            telemetry.condition(connection, 75, 'TEST', 'concurrent', True, 'new fault')
        with closing(self.connect()) as connection:
            try:
                with connection:
                    telemetry.condition(connection, 75, 'TEST', 'rollback', True, 'not committed')
                    raise RuntimeError('rollback')
            except RuntimeError:
                pass
            self.assertEqual(connection.execute('SELECT count(*) FROM public.tb_alarm').fetchone()[0], 2)
            self.assertEqual(connection.execute('SELECT count(*) FROM public.tb_log').fetchone()[0], 3)

    def test_postgres_receive_hooks_write_business_data_and_device_logs(self):
        with closing(self.connect()) as connection, connection:
            connection.execute('''
                CREATE TABLE IF NOT EXISTS public.tb_python_device_mapping (
                    data_kind TEXT NOT NULL, external_key TEXT NOT NULL,
                    device_id BIGINT NOT NULL REFERENCES public.tb_device,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE, PRIMARY KEY(data_kind,external_key));
                INSERT INTO public.tb_python_device_mapping VALUES ('DCS','44/56/01',75,TRUE)
                    ON CONFLICT DO NOTHING;
                INSERT INTO public.tb_python_device_mapping VALUES ('KA','cam_pg',75,TRUE)
                    ON CONFLICT DO NOTHING;
                CREATE TABLE IF NOT EXISTS public.tb_dcs_data (
                    dcs_data_id BIGSERIAL PRIMARY KEY, device_id BIGINT REFERENCES public.tb_device,
                    data_payload VARCHAR(500), received_status VARCHAR(20), received_at TIMESTAMP);
                CREATE TABLE IF NOT EXISTS public.tb_dcs_receive_details (
                    id BIGSERIAL PRIMARY KEY, dcs_data_id BIGINT REFERENCES public.tb_dcs_data,
                    received_at TIMESTAMPTZ, source TEXT, raw_hex TEXT, parse_status TEXT, parse_error TEXT,
                    station_number TEXT, system_group_number TEXT, group_number TEXT, field_8_9 TEXT,
                    voltage TEXT, parity_1 TEXT, field_13 TEXT, quantity_1 TEXT, parity_2 TEXT,
                    field_21 TEXT, quantity_2 TEXT, quantity_3 TEXT, quantity_4 TEXT,
                    error_code_hex TEXT, document_xor_hex TEXT, document_xor_matches BOOLEAN,
                    mapping_error TEXT);
                CREATE TABLE IF NOT EXISTS public.tb_ka_data (
                    ka_data_id BIGSERIAL PRIMARY KEY, device_id BIGINT REFERENCES public.tb_device,
                    file_path VARCHAR(500), received_at TIMESTAMP, file_size BIGINT,
                    file_name VARCHAR(255), receive_status VARCHAR(20));
                CREATE TABLE IF NOT EXISTS public.tb_ka_receive_details (
                    ka_data_id BIGINT REFERENCES public.tb_ka_data, camera_id TEXT, source_path TEXT,
                    marker_path TEXT, marker_mtime_ns BIGINT, marker_size BIGINT, sha256 TEXT);
            ''')
        with closing(self.connect()) as connection:
            store = FrameStore.__new__(FrameStore)
            store._connection = connection
            raw = bytes.fromhex(SAMPLE_HEX)
            row_id = store.save(source='tcp:test', raw=raw, parsed=parse_frame(raw), error=None)
            self.assertIsNotNone(row_id)
            received = datetime.now(timezone.utc).isoformat()
            ka_id = db.save_ka(connection, (received, 'cam_pg', 'clip.mp4', 'clip.dump',
                                           1, 0, 'archive.mp4', 10, 'a' * 64))
            self.assertIsNotNone(ka_id)
            logs = connection.execute('SELECT device_id, message FROM public.tb_log ORDER BY id').fetchall()
            self.assertEqual(len(logs), 2)
            self.assertEqual(logs[0][0], 75)
            self.assertIn('DCS_RECEIVE', logs[0][1])
            self.assertIn('KA_RECEIVE', logs[1][1])
            self.assertEqual(connection.execute('SELECT count(*) FROM public.tb_alarm').fetchone()[0], 0)
