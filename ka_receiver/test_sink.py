"""Local-only durable endpoint for the provisional Ka video upload protocol."""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import unquote

from .transport import CHUNK_SIZE
from .filenames import original_filename


def make_test_sink(output_dir: Path, port: int = 8766, max_bytes: int = 10 * 1024**3) -> HTTPServer:
    if max_bytes < 1:
        raise ValueError('max_bytes must be positive')
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    database = output_dir / 'uploads.sqlite3'
    with closing(sqlite3.connect(database)) as db:
        db.execute('''CREATE TABLE IF NOT EXISTS uploads (
            delivery_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, byte_size INTEGER NOT NULL,
            path TEXT NOT NULL, metadata_json TEXT NOT NULL
        )''')
        db.commit()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.connection.settimeout(30)
            if self.path != '/ka':
                self.send_error(404)
                return
            delivery_id = self.headers.get('Idempotency-Key', '')
            expected_hash = self.headers.get('X-Content-SHA256', '')
            if not re.fullmatch('[0-9a-f]{32}', delivery_id) or not re.fullmatch('[0-9a-f]{64}', expected_hash):
                self.send_error(400, 'Valid delivery ID and SHA-256 required')
                return
            try:
                size = int(self.headers.get('Content-Length', ''))
            except ValueError:
                self.send_error(411)
                return
            if size < 1 or size > max_bytes or self.headers.get('Transfer-Encoding'):
                self.send_error(413)
                return
            metadata = {
                'camera_id': unquote(self.headers.get('X-Ka-Camera-Id', '')),
                'receipt_id': self.headers.get('X-Ka-Receipt-Id', ''),
                'filename': unquote(self.headers.get('X-Ka-Filename', '')),
                'received_at': self.headers.get('X-Ka-Received-At', ''),
            }
            if not all(metadata.values()):
                self.send_error(400, 'Video metadata required')
                return
            metadata_json = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
            try:
                filename = original_filename(metadata['filename'])
            except ValueError:
                self.send_error(400, 'Invalid original filename')
                return
            delivery_dir = output_dir / delivery_id
            destination = delivery_dir / filename
            staging = None
            try:
                with tempfile.NamedTemporaryFile(dir=output_dir, suffix='.part', delete=False) as target:
                    staging = Path(target.name)
                    remaining = size
                    digest = hashlib.sha256()
                    while remaining:
                        chunk = self.rfile.read(min(CHUNK_SIZE, remaining))
                        if not chunk:
                            self.send_error(400, 'Incomplete upload')
                            return
                        target.write(chunk)
                        digest.update(chunk)
                        remaining -= len(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                if digest.hexdigest() != expected_hash:
                    self.send_error(422, 'SHA-256 mismatch')
                    return
                with closing(sqlite3.connect(database, timeout=10)) as db:
                    existing = db.execute(
                        'SELECT sha256, byte_size, metadata_json, path FROM uploads WHERE delivery_id = ?',
                        (delivery_id,),
                    ).fetchone()
                    if existing and existing[:3] != (expected_hash, size, metadata_json):
                        self.send_error(409, 'Delivery ID belongs to different content')
                        return
                    if existing:
                        # Keep completed pre-upgrade uploads at their recorded path on retry.
                        stored = Path(existing[3])
                        legacy_suffix = Path(filename).suffix
                        if not re.fullmatch(r'\.[A-Za-z0-9]{1,10}', legacy_suffix):
                            legacy_suffix = '.bin'
                        legacy = output_dir / (delivery_id + legacy_suffix.lower())
                        if stored not in (destination, legacy):
                            raise OSError('Stored upload path is outside the expected layout')
                        destination = stored
                    if destination.parent != output_dir:
                        delivery_dir.mkdir(exist_ok=True)
                        if delivery_dir.is_symlink() or delivery_dir.resolve() != delivery_dir:
                            raise OSError('Unsafe delivery directory')
                    # A retry uses the same target path, including after an interrupted commit.
                    os.replace(staging, destination)
                    db.execute('INSERT OR IGNORE INTO uploads VALUES (?, ?, ?, ?, ?)',
                               (delivery_id, expected_hash, size, str(destination), metadata_json))
                    db.commit()
                ack = {'stored': True, 'delivery_id': delivery_id, 'byte_size': size,
                       'sha256': expected_hash, 'filename': filename}
                body = json.dumps(ack).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                print(json.dumps(ack), flush=True)
            except (OSError, sqlite3.Error):
                try:
                    self.send_error(500, 'Upload storage failed')
                except OSError:
                    pass
            finally:
                if staging:
                    staging.unlink(missing_ok=True)

        def log_message(self, *_):
            pass

    return HTTPServer(('127.0.0.1', port), Handler)


def main():
    parser = argparse.ArgumentParser(description='Local HTTP test destination for Ka CCTV files')
    parser.add_argument('--output-dir', type=Path,
                        default=Path(__file__).resolve().parent.parent / 'data' / 'ka_test_received')
    parser.add_argument('--port', type=int, default=8766)
    parser.add_argument('--max-bytes', type=int, default=10 * 1024**3)
    args = parser.parse_args()
    try:
        with make_test_sink(args.output_dir, args.port, args.max_bytes) as server:
            print(f'Ka test endpoint: http://127.0.0.1:{server.server_port}/ka', flush=True)
            server.serve_forever()
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f'Test server error: {exc}\n')


if __name__ == '__main__':
    main()
