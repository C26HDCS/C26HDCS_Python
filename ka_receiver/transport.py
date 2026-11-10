"""Provisional streaming HTTP protocol for completed CCTV files."""

import hashlib
import http.client
import json
from pathlib import Path
from urllib.parse import quote, urlsplit


CHUNK_SIZE = 1024 * 1024


class DeliveryError(Exception):
    def __init__(self, message: str, response_code: int | None = None):
        super().__init__(message)
        self.response_code = response_code


def validate_url(url: str):
    parsed = urlsplit(url)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username is not None or parsed.password is not None or parsed.fragment
            or any(ord(c) <= 32 for c in url)):
        raise ValueError('url must be an HTTP(S) endpoint without credentials or a fragment')
    _ = parsed.port
    return parsed


def upload_file(url: str, job: dict, timeout_seconds: int, progress) -> int:
    """Stream bounded chunks; require a matching durable-storage acknowledgement."""
    parsed = validate_url(url)
    connection_type = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
    connection = connection_type(parsed.hostname, parsed.port, timeout=timeout_seconds)
    try:
        with Path(job['archive_path']).open('rb') as source:
            digest = hashlib.sha256()
            size = 0
            while chunk := source.read(CHUNK_SIZE):
                digest.update(chunk)
                size += len(chunk)
                progress()
            if size != job['byte_size'] or digest.hexdigest() != job['sha256']:
                raise DeliveryError('Archived video size or SHA-256 does not match receipt')
            source.seek(0)
            path = parsed.path or '/'
            if parsed.query:
                path += '?' + parsed.query
            connection.putrequest('POST', path)
            headers = {
                'Content-Type': 'application/octet-stream',
                'Content-Length': str(size),
                'Idempotency-Key': job['delivery_id'],
                'X-Content-SHA256': job['sha256'],
                'X-Ka-Camera-Id': quote(job['camera_id'], safe=''),
                'X-Ka-Receipt-Id': str(job['receipt_id']),
                'X-Ka-Filename': quote(Path(job['source_path']).name, safe=''),
                'X-Ka-Received-At': job['received_at'],
            }
            for key, value in headers.items():
                connection.putheader(key, value)
            connection.endheaders()
            remaining = size
            sent_digest = hashlib.sha256()
            while remaining:
                chunk = source.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    raise DeliveryError('Archived video was truncated during upload')
                connection.send(chunk)
                remaining -= len(chunk)
                sent_digest.update(chunk)
                progress()
            if source.read(1) or sent_digest.hexdigest() != job['sha256']:
                raise DeliveryError('Archived video changed during upload')
            response = connection.getresponse()
            if not 200 <= response.status < 300:
                raise DeliveryError(f'HTTP {response.status}', response.status)
            body = response.read(65537)
            if len(body) > 65536:
                raise DeliveryError('Acknowledgement exceeds 64 KiB', response.status)
            try:
                ack = json.loads(body)
            except (ValueError, UnicodeError) as exc:
                raise DeliveryError('Invalid JSON acknowledgement', response.status) from exc
            if (not isinstance(ack, dict) or ack.get('stored') is not True
                    or ack.get('delivery_id') != job['delivery_id']
                    or type(ack.get('byte_size')) is not int or ack['byte_size'] != size
                    or ack.get('sha256') != job['sha256']):
                raise DeliveryError('Storage acknowledgement does not match video', response.status)
            return response.status
    except (OSError, http.client.HTTPException) as exc:
        raise DeliveryError(str(exc)) from exc
    finally:
        connection.close()
