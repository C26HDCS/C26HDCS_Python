"""Shared SQLite tables for receiver progress and pending file pairs."""

import hdcs_db as db


def initialize(connection: db.Connection) -> None:
    connection.initialize("""
        CREATE TABLE IF NOT EXISTS ka_receiver_runtime (
            camera_id TEXT PRIMARY KEY,
            watch_dir TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            state TEXT NOT NULL,
            error_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        )
    """)
    connection.initialize("""
        CREATE TABLE IF NOT EXISTS ka_receiver_pending (
            camera_id TEXT NOT NULL,
            item TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            detail TEXT NOT NULL,
            PRIMARY KEY (camera_id, item)
        )
    """)
    connection.commit()
