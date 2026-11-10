import socket
import os
from unittest.mock import patch
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path

from dcs_receiver.__main__ import SAMPLE_HEX, make_tcp_server
from dcs_receiver.protocol import FrameDecoder, FrameError, parse_frame
from dcs_receiver.receiver import Ingestor
from dcs_receiver.storage import FrameStore


SAMPLE = bytes.fromhex(SAMPLE_HEX)


def setUpModule():
    mode = patch.dict(os.environ, {'HDCS_DB_BACKEND': 'sqlite'})
    mode.start()
    unittest.addModuleCleanup(mode.stop)


class ProtocolTests(unittest.TestCase):
    def test_pdf_sample_fields_and_unverified_xor(self) -> None:
        frame = parse_frame(SAMPLE)
        self.assertEqual((frame.system_group_number, frame.group_number, frame.station_number), ("44", "56", "01"))
        self.assertEqual(frame.voltage, "12.50")
        self.assertEqual(
            (frame.quantity_1, frame.quantity_2, frame.quantity_3, frame.quantity_4),
            ("0443", "0060", "0445", "0000"),
        )
        self.assertEqual(frame.document_xor_hex, "78")
        self.assertEqual(frame.error_code_hex, "56")
        self.assertFalse(frame.document_xor_matches)

    def test_split_and_concatenated_tcp_chunks(self) -> None:
        decoder = FrameDecoder()
        self.assertEqual(decoder.feed(b"noise" + SAMPLE[:9]), [])
        frames = decoder.feed(SAMPLE[9:] + SAMPLE)
        self.assertEqual([candidate.raw for candidate in frames], [SAMPLE, SAMPLE])
        self.assertEqual(decoder.noise_bytes, 5)
        self.assertEqual(decoder.finish(), [])

    def test_recovery_after_truncated_and_overlong_frames(self) -> None:
        decoder = FrameDecoder()
        frames = decoder.feed(SAMPLE[:10] + SAMPLE)
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0].framing_error, "new STX before CR")
        self.assertEqual(frames[1].raw, SAMPLE)

        decoder = FrameDecoder()
        frames = decoder.feed(SAMPLE[:-1] + b"X" + SAMPLE)
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0].framing_error, "byte 35 is not CR")
        self.assertEqual(frames[1].raw, SAMPLE)

    def test_invalid_quantity_is_rejected(self) -> None:
        corrupted = bytearray(SAMPLE)
        corrupted[13] = ord("A")
        with self.assertRaisesRegex(FrameError, "quantity 1"):
            parse_frame(bytes(corrupted))


class StorageTests(unittest.TestCase):
    def test_raw_frames_are_saved_for_valid_and_invalid_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.sqlite3"
            corrupted = bytearray(SAMPLE)
            corrupted[13] = ord("A")
            with FrameStore(path) as store:
                ingestor = Ingestor(store, "test-input")
                receipts = ingestor.feed(SAMPLE + bytes(corrupted))
                self.assertEqual(len(receipts), 2)
                self.assertIsNotNone(receipts[0].parsed)
                self.assertIsNone(receipts[1].parsed)

            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute(
                    "SELECT raw_hex, parse_status, parse_error, document_xor_matches "
                    "FROM dcs_frames ORDER BY id"
                ).fetchall()
            self.assertEqual(rows[0], (SAMPLE.hex().upper(), "PARSED", None, 0))
            self.assertEqual(rows[1][0], bytes(corrupted).hex().upper())
            self.assertEqual(rows[1][1], "INVALID")
            self.assertIn("quantity 1", rows[1][2])

    def test_tcp_listener_receives_split_frame(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "frames.sqlite3"
            with make_tcp_server(path, "127.0.0.1", 0) as server:
                server_thread = threading.Thread(target=server.serve_forever)
                server_thread.start()
                try:
                    with socket.create_connection(server.server_address, timeout=3) as client:
                        client.sendall(SAMPLE[:11])
                        client.sendall(SAMPLE[11:])
                finally:
                    server.shutdown()
                    server_thread.join(timeout=3)

            with closing(sqlite3.connect(path)) as connection:
                row = connection.execute(
                    "SELECT raw_hex, parse_status FROM dcs_frames"
                ).fetchone()
            self.assertEqual(row, (SAMPLE.hex().upper(), "PARSED"))


if __name__ == "__main__":
    unittest.main()
