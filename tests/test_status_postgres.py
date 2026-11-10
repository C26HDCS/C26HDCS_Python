"""Only runs against a disposable local PostgreSQL database."""
import os
from pathlib import Path
import unittest
from contextlib import closing
from datetime import datetime, timezone
import hdcs_db as db
import hdcs_status as status


@unittest.skipUnless(os.getenv('HDCS_TEST_STATUS_POSTGRES') == '1', 'Requires disposable local PostgreSQL')
class DeviceStatusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings = db.postgres_settings()
        if settings['host'] not in ('localhost', '127.0.0.1') or not settings['dbname'].startswith('hdcs_test_'):
            raise RuntimeError('Disposable local hdcs_test_* database only')
        with db.open_postgres() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS public.tb_device(device_id BIGINT PRIMARY KEY,
                device_status VARCHAR(20) NOT NULL DEFAULT 'NORMAL',last_received_at TIMESTAMP,last_receive_status VARCHAR(20));
                CREATE TABLE IF NOT EXISTS public.tb_device_status(status_id BIGSERIAL PRIMARY KEY,device_id BIGINT,
                device_status VARCHAR(20),status_message VARCHAR(500),checked_at TIMESTAMP);
                CREATE TABLE IF NOT EXISTS public.tb_log(id BIGSERIAL PRIMARY KEY,device_id BIGINT,level VARCHAR(10),message VARCHAR(1000),reg_dt TIMESTAMP);
                CREATE TABLE IF NOT EXISTS public.tb_alarm(alarm_id BIGSERIAL PRIMARY KEY,device_id BIGINT,alarm_type VARCHAR(50),alarm_level VARCHAR(20),alarm_message VARCHAR(500),occurred_at TIMESTAMP);
                CREATE TABLE IF NOT EXISTS public.tb_python_alarm_state(device_id BIGINT,incident_key VARCHAR(64),active BOOLEAN,updated_at TIMESTAMP,PRIMARY KEY(device_id,incident_key));
                CREATE TABLE IF NOT EXISTS public.tb_python_device_mapping(data_kind VARCHAR(3),external_key VARCHAR(255),device_id BIGINT,enabled BOOLEAN DEFAULT TRUE,PRIMARY KEY(data_kind,external_key));
                INSERT INTO public.tb_device(device_id) VALUES(75),(86) ON CONFLICT DO NOTHING;
                INSERT INTO public.tb_python_device_mapping VALUES('DCS','44/56/01',75,TRUE),('KA','cam_test',86,TRUE) ON CONFLICT DO NOTHING;''')
            sql = Path(os.environ['HDCS_STATUS_SQL']).read_text(encoding='utf-8')
            c.execute(sql)
            c.execute(sql)
            lifecycle = Path(os.environ['HDCS_STATUS_SQL']).with_name('python_status_lifecycle_postgresql.sql').read_text(encoding='utf-8')
            c.execute(lifecycle)
            c.execute(lifecycle)

    def connection(self):
        c = db.Connection.__new__(db.Connection)
        c.is_postgres = True
        c.raw = db.open_postgres()
        return c

    def setUp(self):
        with db.open_postgres() as c:
            c.execute("UPDATE tb_python_device_mapping SET device_id=CASE WHEN data_kind='DCS' THEN 75 ELSE 86 END,enabled=TRUE")
            c.execute('DELETE FROM tb_python_component_status; DELETE FROM tb_python_alarm_state; DELETE FROM tb_alarm; DELETE FROM tb_log; DELETE FROM tb_device_status; UPDATE tb_device SET device_status=\'NORMAL\',last_received_at=NULL,last_receive_status=NULL')
            c.execute('DELETE FROM tb_python_delivery_target')

    def test_mapping_move_and_disable_retire_only_owned_live_states(self):
        with closing(self.connection()) as c,c:
            status.publish(c,86,'KA','MONITOR','cam_test','ERROR','old failure',ttl=120)
        with db.open_postgres() as c:
            c.execute("UPDATE tb_python_device_mapping SET device_id=75 WHERE data_kind='KA' AND external_key='cam_test'")
        self.assertEqual(self.value('SELECT count(*) FROM tb_python_component_status WHERE device_id=86'),0)
        self.assertEqual(self.value('SELECT device_status FROM tb_device WHERE device_id=86'),'WARNING')
        self.assertEqual(self.value("SELECT count(*) FROM tb_python_component_status WHERE device_id=75 AND data_kind='KA'"),1)
        with db.open_postgres() as c:
            c.execute("UPDATE tb_python_device_mapping SET enabled=FALSE WHERE data_kind='KA'")
        self.assertEqual(self.value("SELECT count(*) FROM v_python_component_health WHERE data_kind='KA'"),0)

    def test_destination_retirement_keeps_alarm_history_and_other_destination(self):
        with closing(self.connection()) as c,c:
            status.publish(c,75,'DCS','DELIVERY','job1','WARNING','retry',owner_key='44/56/01',destination='retired')
            status.publish(c,75,'DCS','DCS_SENDER_RUNTIME','*|active','NORMAL','responsive',ttl=120,destination='active')
        with db.open_postgres() as c:
            c.execute("INSERT INTO tb_alarm(device_id,alarm_type) VALUES(75,'historical')")
            c.execute("UPDATE tb_python_delivery_target SET enabled=FALSE WHERE destination_id='retired'")
        self.assertEqual(self.value("SELECT count(*) FROM tb_python_component_status WHERE destination_id='retired'"),0)
        self.assertEqual(self.value("SELECT count(*) FROM tb_python_component_status WHERE destination_id='active'"),1)
        self.assertEqual(self.value('SELECT count(*) FROM tb_alarm'),1)
        with closing(self.connection()) as c:
            with self.assertRaises(ValueError): status.ensure_destination(c,'DCS','retired')

    def value(self, sql):
        with db.open_postgres() as c:
            return c.execute(sql).fetchone()[0]

    def test_one_healthy_component_cannot_erase_other_fault(self):
        with closing(self.connection()) as c, c:
            status.publish(c,75,'DCS','DELIVERY','job1','WARNING','retry')
            status.publish(c,75,'DCS','MONITOR','44/56/01','NORMAL','receiving',ttl=120)
        self.assertEqual(self.value('SELECT device_status FROM tb_device WHERE device_id=75'),'WARNING')
        with closing(self.connection()) as c, c:
            status.publish(c,75,'DCS','DELIVERY','job2','NORMAL','sent')
        self.assertEqual(self.value('SELECT device_status FROM tb_device WHERE device_id=75'),'WARNING')
        with closing(self.connection()) as c, c:
            status.publish(c,75,'DCS','DELIVERY','job1','NORMAL','sent')
        self.assertEqual(self.value('SELECT device_status FROM tb_device WHERE device_id=75'),'NORMAL')
        self.assertEqual(self.value('SELECT count(*) FROM tb_device_status'),2)

    def test_web_timeout_alarm_dedup_and_recovery_without_python(self):
        with closing(self.connection()) as c, c:
            status.publish(c,75,'DCS','MONITOR','44/56/01','NORMAL','receiving',ttl=120)
        with db.open_postgres() as c:
            c.execute("UPDATE tb_python_component_status SET expires_at=CURRENT_TIMESTAMP-INTERVAL '1 second'")
            for _ in range(3): c.execute('SELECT public.hdcs_refresh_python_device_status(75)')
        self.assertEqual(self.value('SELECT device_status FROM tb_device WHERE device_id=75'),'ERROR')
        self.assertEqual(self.value('SELECT count(*) FROM tb_alarm'),1)
        with closing(self.connection()) as c, c:
            status.publish(c,75,'DCS','MONITOR','44/56/01','NORMAL','recovered',ttl=120)
        self.assertEqual(self.value('SELECT device_status FROM tb_device WHERE device_id=75'),'NORMAL')
        self.assertEqual(self.value('SELECT count(*) FROM tb_alarm'),1)
        self.assertEqual(self.value("SELECT count(*) FROM tb_log WHERE level='INFO'"),1)

    def test_receipt_time_does_not_go_back_or_change_during_monitoring(self):
        with closing(self.connection()) as c, c:
            status.receipt(c,75,'2026-10-23T01:00:00+00:00')
            status.receipt(c,75,'2026-10-22T01:00:00+00:00','FAIL')
            status.publish(c,75,'DCS','MONITOR','44/56/01','WARNING','stale',ttl=120)
        self.assertEqual(str(self.value('SELECT last_received_at FROM tb_device WHERE device_id=75')),'2026-10-23 10:00:00')
        self.assertEqual(self.value('SELECT last_receive_status FROM tb_device WHERE device_id=75'),'SUCCESS')

    def test_transaction_rollback_has_no_device_or_history_side_effects(self):
        with closing(self.connection()) as c:
            try:
                with c:
                    status.publish(c,75,'DCS','MONITOR','44/56/01','ERROR','fault')
                    raise RuntimeError('rollback')
            except RuntimeError: pass
        self.assertEqual(self.value('SELECT device_status FROM tb_device WHERE device_id=75'),'NORMAL')
        self.assertEqual(self.value('SELECT count(*) FROM tb_device_status'),0)

    def test_runtime_without_single_device_mapping_heartbeats_all_devices_of_band(self):
        with db.open_postgres() as c:
            c.execute("INSERT INTO tb_python_device_mapping VALUES('DCS','44/56/02',86,TRUE) ON CONFLICT DO NOTHING")
        try:
            with closing(self.connection()) as c,c:
                status.runtime(c,'DCS','DCS_SENDER_RUNTIME',None,'local',True)
            self.assertEqual(self.value("SELECT count(*) FROM tb_python_component_status WHERE component='DCS_SENDER_RUNTIME'"),2)
        finally:
            with db.open_postgres() as c: c.execute("DELETE FROM tb_python_device_mapping WHERE data_kind='DCS' AND external_key='44/56/02'")

    def test_sqlite_trial_remains_independent(self):
        class Trial: is_postgres=False
        status.publish(Trial(),75,'DCS','MONITOR','','NORMAL','trial')
        status.receipt(Trial(),75,'2026-10-23T01:00:00+00:00')
