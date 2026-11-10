"""SQLite persistence for raw and parsed DCS frames."""

import hdcs_db as db
from datetime import datetime, timezone
from pathlib import Path

from .protocol import ParsedFrame


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS dcs_frames (
    id INTEGER PRIMARY KEY,
    received_at TEXT NOT NULL,
    source TEXT NOT NULL,
    raw_hex TEXT NOT NULL,
    parse_status TEXT NOT NULL,
    parse_error TEXT,
    station_number TEXT,
    system_group_number TEXT,
    group_number TEXT,
    field_8_9 TEXT,
    voltage TEXT,
    parity_1 TEXT,
    field_13 TEXT,
    quantity_1 TEXT,
    parity_2 TEXT,
    field_21 TEXT,
    quantity_2 TEXT,
    quantity_3 TEXT,
    quantity_4 TEXT,
    error_code_hex TEXT,
    document_xor_hex TEXT,
    document_xor_matches INTEGER
)
"""


class FrameStore:
    def __init__(self, path: Path) -> None:
        self._connection = db.connect(path, timeout=10)
        try:
            self._connection.initialize(CREATE_TABLE)
            self._connection.commit()
        except Exception:
            self._connection.close()
            raise
        self.last_mapping_error = None

    def save(
        self,
        *,
        source: str,
        raw: bytes,
        parsed: ParsedFrame | None,
        error: str | None,
    ) -> int:
        values = (
            datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            source,
            raw.hex().upper(),
            "PARSED" if parsed else "INVALID",
            error,
            parsed.station_number if parsed else None,
            parsed.system_group_number if parsed else None,
            parsed.group_number if parsed else None,
            parsed.field_8_9 if parsed else None,
            parsed.voltage if parsed else None,
            parsed.parity_1 if parsed else None,
            parsed.field_13 if parsed else None,
            parsed.quantity_1 if parsed else None,
            parsed.parity_2 if parsed else None,
            parsed.field_21 if parsed else None,
            parsed.quantity_2 if parsed else None,
            parsed.quantity_3 if parsed else None,
            parsed.quantity_4 if parsed else None,
            parsed.error_code_hex if parsed else None,
            parsed.document_xor_hex if parsed else None,
            int(parsed.document_xor_matches) if parsed else None,
        )
        if self._connection.is_postgres:
            try:
                row_id, self.last_mapping_error = db.save_dcs(self._connection, values)
                return row_id
            except Exception:
                self._connection.rollback()
                raise
        cursor = self._connection.execute(
            """
            INSERT INTO dcs_frames (
                received_at, source, raw_hex, parse_status, parse_error,
                station_number, system_group_number, group_number, field_8_9,
                voltage, parity_1, field_13, quantity_1, parity_2, field_21,
                quantity_2, quantity_3, quantity_4, error_code_hex,
                document_xor_hex, document_xor_matches
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            values,
        )
        from hdcs_telemetry import record, device_for, condition, DATA
        if not self._connection.is_postgres:
            with self._connection:
                record(self._connection, device_for(self._connection, 'DCS'), 'DCS_RECEIVE',
                       'Frame stored' if parsed else 'Invalid frame stored', 'INFO' if parsed else 'WARN')
                condition(self._connection, device_for(self._connection, 'DCS'),
                          'DCS_RECEIVE_INVALID', 'receiver', parsed is None,
                          error or 'Valid frame received', category=DATA)
        self._connection.commit()
        return cursor.lastrowid

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "FrameStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
