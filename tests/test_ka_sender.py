import json
import hashlib
import http.client
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

from ka_receiver.receiver import KaReceiver
from ka_receiver.sender import KaSender
from ka_receiver.test_sink import make_test_sink
from ka_receiver.transport import DeliveryError, upload_file
from ka_receiver.filenames import original_filename


def setUpModule():
    mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite'})
    mode.start()
    unittest.addModuleCleanup(mode.stop)


class KaSenderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / 'ka.sqlite3'
        self.incoming = self.root / 'incoming'
        self.incoming.mkdir()
        self.target = self.root / 'target'
        self.server = make_test_sink(self.target, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/ka'

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()
        self.temp.cleanup()

    def receive(self, camera='cam01', name='clip', data=b'video bytes'):
        (self.incoming / (name + '.mp4')).write_bytes(data)
        (self.incoming / (name + '.dump')).touch()
        with KaReceiver(self.incoming, self.root / 'archive', self.db, camera, settle_seconds=0) as receiver:
            receiver.scan_once()

    def sender(self, **kwargs):
        return KaSender(self.db, destination_id='local', url=self.url, **kwargs)

    def ready_again(self):
        with closing(sqlite3.connect(self.db)) as db:
            db.execute("UPDATE ka_delivery_jobs SET next_attempt_at = '2000', lease_until = '2000'")
            db.commit()

    def test_completed_files_only_stream_and_restart(self):
        payload = b'video' * 500000
        self.receive(data=payload)
        (self.incoming / 'unfinished.mp4').write_bytes(b'partial')
        with self.sender() as sender:
            result = sender.send_ready()
            self.assertEqual((result.enqueued, result.sent, result.retry_scheduled), (1, 1, 0))
        with self.sender() as sender:
            self.assertEqual(sender.send_ready().sent, 0)
        files = list(self.target.glob('*/*.mp4'))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].read_bytes(), payload)
        self.assertEqual(files[0].name, 'clip.mp4')
        self.assertEqual(list(self.target.glob('*.part')), [])

    def test_original_unicode_name_and_uppercase_extension_survive_end_to_end(self):
        filename = '\uD55C\uAC15 CCTV 01.MP4'
        payload = b'original name video'
        (self.incoming / filename).write_bytes(payload)
        (self.incoming / (Path(filename).stem + '.dump')).touch()
        with KaReceiver(self.incoming, self.root / 'archive', self.db, 'cam01', settle_seconds=0) as receiver:
            self.assertEqual(receiver.scan_once()[0].status, 'RECEIVED')
            archive = Path(receiver.connection.execute('SELECT archive_path FROM ka_video_receipts').fetchone()[0])
            self.assertEqual(archive.name, filename)
        with self.sender() as sender:
            self.assertEqual(sender.send_ready().sent, 1)
        with closing(sqlite3.connect(self.target / 'uploads.sqlite3')) as db:
            stored_path, metadata = db.execute('SELECT path,metadata_json FROM uploads').fetchone()
        self.assertEqual(Path(stored_path).name, filename)
        self.assertEqual(Path(stored_path).read_bytes(), payload)
        self.assertEqual(json.loads(metadata)['filename'], filename)

    def test_same_filename_for_multiple_receipts_is_not_overwritten(self):
        self.receive(data=b'first')
        marker = self.incoming / 'clip.dump'
        previous = marker.stat().st_mtime_ns
        (self.incoming / 'clip.mp4').write_bytes(b'second')
        os.utime(marker, ns=(previous - 1000000000, previous - 1000000000))
        with KaReceiver(self.incoming, self.root / 'archive', self.db, 'cam01', settle_seconds=0) as receiver:
            self.assertEqual(receiver.scan_once()[0].status, 'RECEIVED')
        with self.sender() as sender:
            self.assertEqual(sender.send_ready().sent, 2)
        files = list(self.target.glob('*/clip.mp4'))
        self.assertEqual(len(files), 2)
        self.assertEqual({path.read_bytes() for path in files}, {b'first', b'second'})

    def test_remote_unsafe_filename_is_rejected_without_writing_video(self):
        for filename in ('../escape.mp4', 'a/b.mp4', 'a\\b.mp4', 'C:clip.mp4',
                         'NUL.mp4', 'CON.txt', 'clip.mp4.', 'clip.mp4 ', 'x\x00.mp4'):
            with self.subTest(filename=filename):
                with self.assertRaises(ValueError):
                    original_filename(filename)
                client = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
                try:
                    client.request('POST', '/ka', body=b'x', headers={
                        'Idempotency-Key': 'a' * 32, 'X-Content-SHA256': hashlib.sha256(b'x').hexdigest(),
                        'X-Ka-Filename': quote(filename, safe=''), 'X-Ka-Camera-Id': 'cam01',
                        'X-Ka-Receipt-Id': '1', 'X-Ka-Received-At': '2026-10-23T00:00:00Z'
                    })
                    response = client.getresponse()
                    self.assertEqual(response.status, 400)
                    response.read()
                finally:
                    client.close()
        self.assertEqual(list(self.target.rglob('*.mp4')), [])

    def test_legacy_stored_path_remains_idempotent_after_upgrade(self):
        self.receive()
        with self.sender() as sender:
            sender.send_ready()
            job = dict(sender.connection.execute('''SELECT j.delivery_id,r.* FROM ka_delivery_jobs j
                JOIN ka_video_receipts r ON r.id=j.receipt_id''').fetchone())
            job['receipt_id'] = job['id']
        with closing(sqlite3.connect(self.target / 'uploads.sqlite3')) as db:
            path = Path(db.execute('SELECT path FROM uploads').fetchone()[0])
            legacy = self.target / (job['delivery_id'] + '.mp4')
            path.rename(legacy)
            db.execute('UPDATE uploads SET path=?', (str(legacy),))
            db.commit()
        self.assertEqual(upload_file(self.url, job, 5, lambda: None), 200)
        self.assertEqual(legacy.read_bytes(), b'video bytes')
        self.assertEqual(list(self.target.glob('*/*.mp4')), [])

    def test_response_loss_retry_keeps_same_delivery_id(self):
        self.receive()
        ids = []

        def lose_response(url, job, timeout, progress):
            ids.append(job['delivery_id'])
            upload_file(url, job, timeout, progress)
            raise DeliveryError('Response lost after destination stored video')

        with self.sender() as sender:
            with patch('ka_receiver.sender.upload_file', side_effect=lose_response):
                self.assertEqual(sender.send_ready().retry_scheduled, 1)
            self.assertEqual(sender.send_ready().sent, 0)
        self.ready_again()
        with self.sender() as sender:
            self.assertEqual(sender.send_ready().sent, 1)
            job = sender.connection.execute('SELECT * FROM ka_delivery_jobs').fetchone()
            self.assertEqual((job['delivery_id'], job['attempts'], job['status']), (ids[0], 2, 'SENT'))
        with closing(sqlite3.connect(self.target / 'uploads.sqlite3')) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM uploads').fetchone()[0], 1)
        self.assertEqual(len(list(self.target.glob('*/*.mp4'))), 1)

    def test_missing_or_modified_archive_is_not_sent(self):
        self.receive()
        with self.sender() as sender:
            archive = Path(sender.connection.execute('SELECT archive_path FROM ka_video_receipts').fetchone()[0])
            archive.write_bytes(b'bad bytes!!')
            self.assertEqual(sender.send_ready().retry_scheduled, 1)
            archive.unlink()
            self.ready_again()
            self.assertEqual(sender.send_ready().retry_scheduled, 1)
        self.assertEqual(list(self.target.rglob('*.mp4')), [])

    def test_expired_claim_is_recovered_and_live_claim_is_not_stolen(self):
        self.receive()
        with self.sender() as first:
            first.enqueue_new()
            claimed = first._claim()
            with self.sender() as second:
                self.assertEqual(second.send_ready().sent, 0)
                self.ready_again()
                first._renew(claimed)
                self.assertEqual(second.send_ready().sent, 0)
        self.ready_again()
        with self.sender() as restarted:
            self.assertEqual(restarted.send_ready().sent, 1)
            outcomes = [r[0] for r in restarted.connection.execute(
                'SELECT outcome FROM ka_delivery_attempts ORDER BY id')]
            self.assertEqual(outcomes, ['INTERRUPTED', 'SENT'])

    def test_camera_filter_applies_to_existing_queue_and_destination_is_bound(self):
        self.receive(camera='cam01', name='first')
        self.receive(camera='cam02', name='second')
        with self.sender() as sender:
            self.assertEqual(sender.enqueue_new(), 2)
        with self.sender(camera_id='cam02') as sender:
            self.assertEqual(sender.send_ready().sent, 1)
            pending = sender.connection.execute("SELECT COUNT(*) FROM ka_delivery_jobs WHERE status='PENDING'").fetchone()[0]
            self.assertEqual(pending, 1)
        with self.assertRaises(ValueError):
            KaSender(self.db, destination_id='local', url=self.url + '-changed')

    def test_cli_and_sender_before_receiver_start(self):
        with self.sender() as sender:
            self.assertEqual(sender.send_ready().enqueued, 0)
        self.receive()
        result = subprocess.run(
            [sys.executable, '-B', '-m', 'ka_receiver.sender', '--db', str(self.db),
             '--destination-id', 'local', '--url', self.url, '--once'],
            cwd=Path(__file__).resolve().parent.parent, capture_output=True, text=True, timeout=15, check=True,
        )
        self.assertEqual(json.loads(result.stdout)['sent'], 1)

    def test_http_errors_redirects_and_invalid_acknowledgements_retry(self):
        self.receive()
        original_handler = self.server.RequestHandlerClass

        class BadReply(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                code, body = self.server.reply
                self.send_response(code)
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Location', '/other')
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        self.server.RequestHandlerClass = BadReply
        with self.sender() as sender:
            for code, body in [(503, b''), (302, b''), (200, b'not-json'),
                               (200, b'{"stored":true}'), (204, b'')]:
                self.server.reply = (code, body)
                self.ready_again()
                with self.subTest(code=code, body=body):
                    result = sender.send_ready()
                    self.assertEqual((result.sent, result.retry_scheduled), (0, 1))
                    row = sender.connection.execute('SELECT status, response_code FROM ka_delivery_jobs').fetchone()
                    self.assertEqual(tuple(row), ('RETRY', code))
            self.server.RequestHandlerClass = original_handler
            self.ready_again()
            self.assertEqual(sender.send_ready().sent, 1)


if __name__ == '__main__':
    unittest.main()
