"""Bound TCP clients and writers; never hold a DB session while waiting for bytes."""
import socket
import socketserver
import threading
import time
import os
from contextlib import closing

import hdcs_db as db
import hdcs_telemetry as telemetry
from .receiver import Ingestor
from .storage import FrameStore


class TcpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    block_on_close = True

    def __init__(self, database, host, port, print_receipts, *, max_clients=32, db_writers=2,
                 idle_timeout_seconds=120, db_wait_seconds=10, store_factory=FrameStore):
        if min(max_clients, db_writers, idle_timeout_seconds, db_wait_seconds) <= 0:
            raise ValueError('TCP limits must be positive')
        self.database = database
        self.print_receipts = print_receipts
        self.idle_timeout_seconds = idle_timeout_seconds
        self.db_wait_seconds = db_wait_seconds
        self.store_factory = store_factory
        self.writers = threading.BoundedSemaphore(db_writers)
        self.slots = threading.BoundedSemaphore(max_clients)
        self.clients = set()
        self.client_lock = threading.Lock()
        self.closing = False
        self._pulse = 0
        self._pulse_failed = False
        super().__init__((host, port), Handler)

    def process_request(self, request, address):
        with self.client_lock:
            if self.closing or not self.slots.acquire(blocking=False):
                self.shutdown_request(request)
                return
            self.clients.add(request)
        try:
            super().process_request(request, address)
        except Exception:
            with self.client_lock: self.clients.discard(request)
            self.slots.release()
            self.shutdown_request(request)
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            with self.client_lock: self.clients.discard(request)
            self.slots.release()

    def server_close(self):
        with self.client_lock:
            self.closing = True
            clients = list(self.clients)
        for client in clients:
            try: client.shutdown(socket.SHUT_RDWR)
            except OSError: pass
        super().server_close()

    def service_actions(self):
        if os.getenv('HDCS_DB_BACKEND', 'sqlite').lower() != 'postgresql': return
        if time.monotonic() - self._pulse < 5: return
        self._pulse = time.monotonic()
        if not self.writers.acquire(blocking=False): return
        try:
            from hdcs_status import runtime
            with closing(db.connect(self.database)) as connection, connection:
                runtime(connection, 'DCS', 'DCS_RECEIVER_RUNTIME', None,
                        f'tcp:{self.server_address[0]}:{self.server_address[1]}', True)
            self._pulse_failed = False
        except Exception:
            if not self._pulse_failed:
                telemetry.logger().error('DCS receiver heartbeat unavailable; DB reconnect uses backoff')
            self._pulse_failed = True
        finally: self.writers.release()


class ShortLivedStore:
    def __init__(self, server):
        self.server = server
        self.last_mapping_error = None

    def save(self, **kwargs):
        server = self.server
        if not server.writers.acquire(timeout=server.db_wait_seconds):
            raise RuntimeError('DCS storage capacity unavailable; client disconnected')
        try:
            with server.store_factory(server.database) as store:
                row_id = store.save(**kwargs)
                self.last_mapping_error = store.last_mapping_error
                return row_id
        finally: server.writers.release()


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        server = self.server
        self.request.settimeout(min(1,server.idle_timeout_seconds))
        ingestor = Ingestor(ShortLivedStore(server), f'tcp:{self.client_address[0]}:{self.client_address[1]}')
        deadline=time.monotonic()+server.idle_timeout_seconds
        try:
            while not server.closing:
                try: chunk=self.request.recv(4096)
                except socket.timeout:
                    if time.monotonic()>=deadline: break
                    continue
                if not chunk: break
                deadline=time.monotonic()+server.idle_timeout_seconds
                server.print_receipts(ingestor.feed(chunk))
            server.print_receipts(ingestor.finish())
        except (db.Error, OSError, RuntimeError) as error:
            if server.closing: return
            telemetry.logger().error('DCS client processing failed: %s; connection closed', type(error).__name__)
            if server.writers.acquire(blocking=False):
                try:
                    telemetry.report_failure(server.database, 'DCS', 'DCS_RECEIVER_RUNTIME', error=error)
                finally: server.writers.release()
