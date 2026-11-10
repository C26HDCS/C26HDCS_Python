import socket
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from dcs_receiver.tcp_server import TcpServer
from dcs_receiver.__main__ import SAMPLE_HEX
import hdcs_db as db
import psycopg


class TcpLimitsTests(unittest.TestCase):
    def server(self, **options):
        server=TcpServer(Path('unused.sqlite3'),'127.0.0.1',0,lambda results: None,**options)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02})
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(lambda: (server.shutdown(),thread.join(3)))
        return server

    def wait(self, predicate):
        until=time.monotonic()+3
        while not predicate() and time.monotonic()<until: time.sleep(0.01)
        self.assertTrue(predicate())

    def connect(self, server):
        client=socket.create_connection(server.server_address,timeout=2)
        self.addCleanup(client.close)
        return client

    def test_idle_clients_open_no_database_and_excess_connections_are_closed(self):
        factory=unittest.mock.Mock()
        server=self.server(max_clients=1,store_factory=factory,idle_timeout_seconds=1)
        first=self.connect(server)
        self.wait(lambda:len(server.clients)==1)
        second=self.connect(server)
        self.assertEqual(second.recv(1),b'')
        factory.assert_not_called()
        self.assertEqual(first.recv(1),b'')
        self.wait(lambda:not server.clients)

    def test_writer_count_is_bounded_and_sessions_close_after_each_frame(self):
        lock=threading.Lock()
        counts={'open':0,'peak':0,'saved':0}
        class Store:
            last_mapping_error=None
            def __init__(self,path): pass
            def __enter__(self):
                with lock:
                    counts['open']+=1
                    counts['peak']=max(counts['peak'],counts['open'])
                return self
            def save(self,**kwargs):
                time.sleep(0.03)
                with lock: counts['saved']+=1
                return 1
            def __exit__(self,*args):
                with lock: counts['open']-=1
        server=self.server(max_clients=5,db_writers=1,store_factory=Store)
        clients=[self.connect(server) for _ in range(5)]
        for client in clients:
            client.sendall(bytes.fromhex(SAMPLE_HEX))
        self.wait(lambda:counts['saved']==5)
        self.assertEqual(counts['peak'],1)
        self.assertEqual(counts['open'],0)

    def test_server_close_unblocks_idle_handlers(self):
        server=self.server(idle_timeout_seconds=3600)
        client=self.connect(server)
        self.wait(lambda:len(server.clients)==1)
        server.shutdown()
        start=time.monotonic()
        server.server_close()
        self.assertLess(time.monotonic()-start,2)
        self.assertEqual(client.recv(1),b'')


class ConnectionBackoffTests(unittest.TestCase):
    def setUp(self):
        db._retry_at=0
        db._retry_delay=0
        self.addCleanup(self.reset)
    def reset(self):
        db._retry_at=0
        db._retry_delay=0
    def test_failed_connections_pause_then_recover(self):
        with patch.object(db,'postgres_settings',return_value={}), patch.object(db.psycopg,'connect') as connect, patch.object(db.time,'monotonic') as clock:
            clock.return_value=0
            connect.side_effect=psycopg.OperationalError('offline')
            with self.assertRaises(db.Error): db.open_postgres()
            with self.assertRaises(db.Error): db.open_postgres()
            self.assertEqual(connect.call_count,1)
            clock.return_value=1
            with self.assertRaises(db.Error): db.open_postgres()
            self.assertEqual(db._retry_delay,2)
            clock.return_value=3
            connect.side_effect=None
            db.open_postgres()
            self.assertEqual(db._retry_delay,0)
