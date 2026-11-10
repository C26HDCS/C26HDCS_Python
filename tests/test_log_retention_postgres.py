"""Opt-in migration/retention verification on a disposable localhost DB only."""

import os
from pathlib import Path
import unittest
import uuid

import hdcs_db as db
import psycopg


@unittest.skipUnless(os.getenv('HDCS_TEST_RETENTION_POSTGRES') == '1',
                     'Requires disposable localhost hdcs_test_* DB')
class RetentionPostgresTests(unittest.TestCase):
    def test_migration_auth_unknown_actor_and_five_year_delete_protection(self):
        settings = db.postgres_settings()
        if settings['host'] not in ('127.0.0.1', 'localhost') or not settings['dbname'].startswith('hdcs_test_'):
            raise RuntimeError('Disposable localhost hdcs_test_* DB required')
        migration = Path(os.environ['HDCS_RETENTION_SQL']).read_text(encoding='utf-8')
        with db.open_postgres() as connection:
            connection.execute('''
                CREATE TABLE IF NOT EXISTS public.tb_user(user_id VARCHAR(50) PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS public.tb_log(
                    id BIGSERIAL PRIMARY KEY, device_id BIGINT, level VARCHAR(10), message VARCHAR(1000),
                    reg_dt TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP);
                CREATE TABLE IF NOT EXISTS public.tb_event(
                    id BIGSERIAL PRIMARY KEY, user_id VARCHAR(50) NOT NULL REFERENCES public.tb_user,
                    event_type VARCHAR(30), message VARCHAR(1000),
                    reg_dt TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP);
            ''')
            connection.execute(migration)
            connection.execute(migration)
            actor = 'audit_' + uuid.uuid4().hex
            connection.execute('INSERT INTO public.tb_user VALUES(%s)', (actor,))
            known = connection.execute('''INSERT INTO public.tb_event(user_id,actor_user_id,event_type,message)
                VALUES(%s,%s,'LOGIN_SUCCESS','test') RETURNING id''', (actor, actor)).fetchone()[0]
            key = str(uuid.uuid4())
            connection.execute('''INSERT INTO public.tb_event(record_key,user_id,actor_user_id,event_type,message)
                VALUES(%s,(SELECT user_id FROM public.tb_user WHERE user_id='unknown'),'unknown','LOGIN_FAIL','test')''',
                (key,))
            connection.execute('DELETE FROM public.tb_user WHERE user_id=%s', (actor,))
            row = connection.execute('SELECT user_id,actor_user_id FROM public.tb_event WHERE id=%s', (known,)).fetchone()
            self.assertEqual(row, (None, actor))
            with self.assertRaises(psycopg.errors.RaiseException):
                connection.execute('DELETE FROM public.tb_event WHERE id=%s', (known,))
            with self.assertRaises(psycopg.errors.RaiseException):
                connection.execute("UPDATE public.tb_event SET reg_dt='2000-01-01' WHERE id=%s", (known,))
            with self.assertRaises(psycopg.errors.RaiseException):
                connection.execute('TRUNCATE public.tb_event')
            old = connection.execute('''INSERT INTO public.tb_log(level,message,reg_dt)
                VALUES('INFO','expired',(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date
                    - interval '5 years' - interval '1 day') RETURNING id''').fetchone()[0]
            boundary = connection.execute('''INSERT INTO public.tb_log(level,message,reg_dt)
                VALUES('INFO','retain',(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date
                    - interval '5 years') RETURNING id''').fetchone()[0]
            self.assertEqual(connection.execute('DELETE FROM public.tb_log WHERE id=%s', (old,)).rowcount, 1)
            with self.assertRaises(psycopg.errors.RaiseException):
                connection.execute('DELETE FROM public.tb_log WHERE id=%s', (boundary,))
