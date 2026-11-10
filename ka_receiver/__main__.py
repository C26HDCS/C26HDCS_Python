"""Poll a Ka-band CCTV handoff folder for completed video files."""

import argparse
import hdcs_db as db
import hdcs_telemetry as telemetry
import json
import time
from dataclasses import asdict
from pathlib import Path

from .receiver import DEFAULT_EXTENSIONS, KaReceiver


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Import CCTV videos after matching .dump markers arrive")
    parser.add_argument("--watch-dir", type=Path, required=True, help="Folder containing video and .dump files")
    parser.add_argument("--camera-id", required=True, help="Stable identifier for this CCTV source")
    parser.add_argument("--archive-dir", type=Path, default=PROJECT_ROOT / "data" / "ka_video")
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data" / "ka_receipts.sqlite3")
    parser.add_argument("--settle-seconds", type=float, default=2)
    parser.add_argument("--poll-seconds", type=float, default=2)
    parser.add_argument(
        "--extensions", default=",".join(sorted(DEFAULT_EXTENSIONS)),
        help="Comma-separated video file extensions, e.g. .mp4,.mkv",
    )
    parser.add_argument("--once", action="store_true", help="Scan one time and exit")
    args = parser.parse_args()
    if args.poll_seconds <= 0:
        parser.error("poll-seconds must be positive")
    extensions = frozenset(part.strip().lower() for part in args.extensions.split(","))

    try:
        with KaReceiver(
            args.watch_dir, args.archive_dir, args.db, args.camera_id,
            settle_seconds=args.settle_seconds, extensions=extensions,
        ) as receiver:
            telemetry.report_failure(args.db, 'KA', 'KA_PROCESS_RUNTIME', key=args.camera_id, active=False)
            print(f"Watching {receiver.watch_dir} for camera {args.camera_id}", flush=True)
            previous: dict[str, str] = {}
            while True:
                results = receiver.scan_once()
                for result in results:
                    if result.status == "RECEIVED" or (
                        result.status != "ALREADY_RECEIVED"
                        and previous.get(result.marker) != result.status + result.detail
                    ):
                        print(json.dumps(asdict(result), ensure_ascii=False), flush=True)
                    previous[result.marker] = result.status + result.detail
                if args.once:
                    if not results:
                        print("No .dump markers found", flush=True)
                    return
                time.sleep(args.poll_seconds)
    except (OSError, ValueError, db.Error) as exc:
        telemetry.report_failure(args.db, 'KA', 'KA_PROCESS_RUNTIME', key=args.camera_id, error=exc)
        parser.error(str(exc))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
