"""Run the sample replay, read a byte file, or listen for TCP bytes."""

import argparse
import hdcs_db as db
import hdcs_telemetry as telemetry
import json
import os
import socket
import socketserver
import time
from dataclasses import asdict
from contextlib import closing
from pathlib import Path

from .monitor import DcsMonitor
from .receiver import Ingestor, Receipt
from .sender import DcsSender
from .storage import FrameStore
from .test_sink import run_test_sink


SAMPLE_HEX = (
    "02 30 31 34 34 35 36 31 35 31 32 39 30 30 34 34 33 35 "
    "30 46 30 30 30 36 30 30 34 34 35 30 30 30 30 56 0D"
)
DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "dcs_frames.sqlite3"


def print_receipts(receipts: list[Receipt]) -> None:
    for receipt in receipts:
        if receipt.parsed and receipt.mapping_error:
            print(f"id={receipt.row_id} UNMAPPED {receipt.mapping_error}", flush=True)
            continue
        if receipt.parsed:
            frame = receipt.parsed
            print(
                f"id={receipt.row_id} PARSED "
                f"key={frame.system_group_number}/{frame.group_number}/{frame.station_number} "
                f"voltage={frame.voltage} "
                f"values={frame.quantity_1},{frame.quantity_2},{frame.quantity_3},{frame.quantity_4} "
                f"document_xor_match={frame.document_xor_matches}",
                flush=True,
            )
        else:
            print(f"id={receipt.row_id} INVALID {receipt.error}", flush=True)


def ingest_bytes(store: FrameStore, source: str, payload: bytes) -> None:
    ingestor = Ingestor(store, source)
    print_receipts(ingestor.feed(payload))
    print_receipts(ingestor.finish())


def read_file(store: FrameStore, path: Path) -> None:
    ingestor = Ingestor(store, f"file:{path}")
    with path.open("rb") as stream:
        while chunk := stream.read(4096):
            print_receipts(ingestor.feed(chunk))
    print_receipts(ingestor.finish())


def make_tcp_server(database: Path, host: str, port: int, **limits) -> socketserver.ThreadingTCPServer:
    from .tcp_server import TcpServer
    return TcpServer(database, host, port, print_receipts, **limits)


def listen_tcp(database: Path, host: str, port: int, **limits) -> None:
    with make_tcp_server(database, host, port, **limits) as server:
        telemetry.report_failure(database, 'DCS', 'DCS_PROCESS_RUNTIME', active=False)
        with closing(db.connect(database)) as connection, connection:
            telemetry.record(connection, telemetry.device_for(connection, 'DCS'),
                             'DCS_RECEIVER', f'Listening on {host}:{server.server_address[1]}')
        print(f"Listening on {host}:{server.server_address[1]}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


def send_sample_tcp(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=5) as connection:
        connection.sendall(bytes.fromhex(SAMPLE_HEX))
    print(f"Sent PDF sample to {host}:{port}", flush=True)


def run_monitor(args: argparse.Namespace) -> None:
    source_prefix = None if args.all_sources else args.source_prefix
    monitor = DcsMonitor(
        args.db,
        stale_after_seconds=args.stale_after_seconds,
        source_prefix=source_prefix,
        station_key=args.station_key,
    )
    first_check = True
    try:
        while True:
            result = monitor.check()
            if first_check:
                telemetry.report_failure(args.db, 'DCS', 'DCS_PROCESS_RUNTIME',
                                         key=args.station_key, active=False)
            if first_check or result.changed or args.once:
                print(json.dumps(asdict(result.snapshot), ensure_ascii=False), flush=True)
            if args.once:
                return
            first_check = False
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        pass


def run_sender(args: argparse.Namespace) -> None:
    source_prefix = None if args.all_sources else args.source_prefix
    with DcsSender(
        args.db,
        destination_id=args.destination_id,
        url=args.url,
        source_prefix=source_prefix,
        timeout_seconds=args.timeout_seconds,
        retry_base_seconds=args.retry_base_seconds,
        retry_max_seconds=args.retry_max_seconds,
    ) as sender:
        first_cycle = True
        try:
            while True:
                result = sender.send_ready(max_jobs=args.max_jobs)
                if first_cycle:
                    telemetry.report_failure(args.db, 'DCS', 'DCS_PROCESS_RUNTIME', active=False)
                if first_cycle or result.enqueued or result.sent or result.retry_scheduled:
                    print(json.dumps(asdict(result)), flush=True)
                if args.once:
                    return
                first_cycle = False
                time.sleep(args.poll_seconds)
        except KeyboardInterrupt:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Receive the supplied 35-byte DCS sample format")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite database path")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("sample", help="Replay the PDF sample frame")
    commands.add_parser("hex", help="Replay one or more hex-encoded frames").add_argument("payload")
    commands.add_parser("file", help="Read raw bytes from a file").add_argument("path", type=Path)
    tcp = commands.add_parser("tcp-listen", help="Listen for a TCP byte stream")
    tcp.add_argument("--host", default="127.0.0.1")
    tcp.add_argument("--port", type=int, default=9000)
    tcp.add_argument('--max-clients', type=int, default=32)
    tcp.add_argument('--db-writers', type=int, default=2)
    tcp.add_argument('--idle-timeout-seconds', type=int, default=120)
    sample_tcp = commands.add_parser("sample-tcp", help="Send the PDF sample to a TCP receiver")
    sample_tcp.add_argument("--host", default="127.0.0.1")
    sample_tcp.add_argument("--port", type=int, default=9000)
    sink = commands.add_parser("test-sink", help="Receive HTTP test deliveries on localhost")
    sink.add_argument("--port", type=int, default=8765)
    monitor = commands.add_parser("monitor", help="Check whether live DCS frames keep arriving")
    monitor.add_argument("--once", action="store_true", help="Check once and exit")
    monitor.add_argument("--stale-after-seconds", type=int, default=600)
    monitor.add_argument("--interval-seconds", type=int, default=10)
    monitor.add_argument("--station-key", help="Monitor one station, e.g. 44/56/01")
    source = monitor.add_mutually_exclusive_group()
    source.add_argument("--source-prefix", default="tcp:")
    source.add_argument("--all-sources", action="store_true", help="Include sample and file input")
    sender = commands.add_parser("send", help="Forward parsed DCS frames with durable retry")
    sender.add_argument("--destination-id", required=True, help="Stable name for this destination")
    sender.add_argument("--url", required=True, help="HTTP or HTTPS POST endpoint")
    sender.add_argument("--once", action="store_true", help="Process currently ready jobs and exit")
    sender.add_argument("--max-jobs", type=int, default=100)
    sender.add_argument("--poll-seconds", type=int, default=10)
    sender.add_argument("--timeout-seconds", type=int, default=10)
    sender.add_argument("--retry-base-seconds", type=int, default=30)
    sender.add_argument("--retry-max-seconds", type=int, default=900)
    source = sender.add_mutually_exclusive_group()
    source.add_argument("--source-prefix", default="tcp:")
    source.add_argument("--all-sources", action="store_true", help="Include sample and file input")
    args = parser.parse_args()
    try:
        dispatch(args, parser)
    except (db.Error, OSError, RuntimeError) as exc:
        telemetry.report_failure(args.db, 'DCS', 'DCS_PROCESS_RUNTIME',
                                 key=getattr(args, 'station_key', None), error=exc)
        raise


def dispatch(args, parser):
    if args.command == "send":
        if min(
            args.max_jobs,
            args.poll_seconds,
            args.timeout_seconds,
            args.retry_base_seconds,
            args.retry_max_seconds,
        ) < 1:
            parser.error("send limits and intervals must be positive")
        try:
            run_sender(args)
        except ValueError as exc:
            parser.error(str(exc))
        return

    if args.command == "monitor":
        if args.stale_after_seconds < 1 or args.interval_seconds < 1:
            parser.error("monitor intervals must be positive seconds")
        run_monitor(args)
        return

    if args.command == "tcp-listen":
        listen_tcp(args.db, args.host, args.port, max_clients=args.max_clients,
                   db_writers=args.db_writers, idle_timeout_seconds=args.idle_timeout_seconds)
        return

    if args.command == "sample-tcp":
        send_sample_tcp(args.host, args.port)
        return

    if args.command == "test-sink":
        run_test_sink(args.port)
        return

    with FrameStore(args.db) as store:
        if args.command == "sample":
            ingest_bytes(store, "pdf-sample", bytes.fromhex(SAMPLE_HEX))
        elif args.command == "hex":
            try:
                payload = bytes.fromhex(args.payload)
            except ValueError as exc:
                parser.error(f"invalid hex payload: {exc}")
            ingest_bytes(store, "hex-input", payload)
        else:
            read_file(store, args.path)


if __name__ == "__main__":
    try:
        main()
    except (db.Error, OSError, RuntimeError) as exc:
        raise SystemExit(f'Process error: {exc}') from None
