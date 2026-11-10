import os
from pathlib import Path


def provision(connection):
    connection.execute('''
        ALTER TABLE public.tb_device ADD COLUMN IF NOT EXISTS device_status VARCHAR(20) NOT NULL DEFAULT 'NORMAL';
        ALTER TABLE public.tb_device ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMP;
        ALTER TABLE public.tb_device ADD COLUMN IF NOT EXISTS last_receive_status VARCHAR(20);
        CREATE TABLE IF NOT EXISTS public.tb_device_status(status_id BIGSERIAL PRIMARY KEY,device_id BIGINT,
            device_status VARCHAR(20),status_message VARCHAR(500),checked_at TIMESTAMP);
        CREATE TABLE IF NOT EXISTS public.tb_log(id BIGSERIAL PRIMARY KEY,device_id BIGINT,level VARCHAR(10),message VARCHAR(1000),reg_dt TIMESTAMP);
        CREATE TABLE IF NOT EXISTS public.tb_alarm(alarm_id BIGSERIAL PRIMARY KEY,device_id BIGINT,alarm_type VARCHAR(50),alarm_level VARCHAR(20),alarm_message VARCHAR(500),occurred_at TIMESTAMP);
        CREATE TABLE IF NOT EXISTS public.tb_python_alarm_state(device_id BIGINT,incident_key VARCHAR(64),active BOOLEAN,updated_at TIMESTAMP,PRIMARY KEY(device_id,incident_key));
        CREATE TABLE IF NOT EXISTS public.tb_python_device_mapping(data_kind VARCHAR(3),external_key VARCHAR(255),device_id BIGINT,enabled BOOLEAN DEFAULT TRUE,PRIMARY KEY(data_kind,external_key));
    ''')
    connection.execute(Path(os.environ['HDCS_STATUS_SQL']).read_text(encoding='utf-8'))
    connection.execute(Path(os.environ['HDCS_STATUS_SQL']).with_name('python_status_lifecycle_postgresql.sql').read_text(encoding='utf-8'))
