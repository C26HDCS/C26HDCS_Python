"""Incremental framing and parsing for the supplied 35-byte RTU sample."""

from dataclasses import dataclass


STX = 0x02
CR = 0x0D
FRAME_LENGTH = 35


class FrameError(ValueError):
    """A received candidate cannot be parsed as a sample frame."""


@dataclass(frozen=True)
class FrameCandidate:
    raw: bytes
    framing_error: str | None = None


@dataclass(frozen=True)
class ParsedFrame:
    station_number: str
    system_group_number: str
    group_number: str
    field_8_9: str
    voltage: str
    parity_1: str
    field_13: str
    quantity_1: str
    parity_2: str
    field_21: str
    quantity_2: str
    quantity_3: str
    quantity_4: str
    error_code_hex: str
    document_xor_hex: str
    document_xor_matches: bool


class FrameDecoder:
    """Accept arbitrary chunks; yield complete or malformed frame candidates."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self.noise_bytes = 0

    def feed(self, chunk: bytes) -> list[FrameCandidate]:
        candidates: list[FrameCandidate] = []
        for byte in chunk:
            if not self._buffer:
                if byte == STX:
                    self._buffer.append(byte)
                else:
                    self.noise_bytes += 1
                continue

            if byte == STX:
                candidates.append(FrameCandidate(bytes(self._buffer), "new STX before CR"))
                self._buffer = bytearray((STX,))
                continue

            self._buffer.append(byte)
            if byte == CR:
                candidates.append(FrameCandidate(bytes(self._buffer)))
                self._buffer.clear()
            elif len(self._buffer) == FRAME_LENGTH:
                candidates.append(FrameCandidate(bytes(self._buffer), "byte 35 is not CR"))
                self._buffer.clear()

        return candidates

    def finish(self) -> list[FrameCandidate]:
        if not self._buffer:
            return []
        candidate = FrameCandidate(bytes(self._buffer), "stream ended before CR")
        self._buffer.clear()
        return [candidate]


def parse_frame(raw: bytes) -> ParsedFrame:
    if len(raw) != FRAME_LENGTH:
        raise FrameError(f"expected {FRAME_LENGTH} bytes, received {len(raw)}")
    if raw[0] != STX or raw[-1] != CR:
        raise FrameError("missing STX or CR")

    try:
        body = raw[1:33].decode("ascii")
    except UnicodeDecodeError as exc:
        raise FrameError("non-ASCII field in frame") from exc

    def field(start: int, end: int) -> str:
        return body[start - 2 : end - 1]

    for label, start, end in (
        ("voltage high", 10, 11),
        ("quantity 1", 14, 17),
        ("voltage low", 18, 19),
        ("quantity 2", 22, 25),
        ("quantity 3", 26, 29),
        ("quantity 4", 30, 33),
    ):
        if not field(start, end).isdigit():
            raise FrameError(f"{label} must contain ASCII digits")

    document_xor = 0
    for byte in raw[:25]:
        document_xor ^= byte

    return ParsedFrame(
        station_number=field(2, 3),
        system_group_number=field(4, 5),
        group_number=field(6, 7),
        field_8_9=field(8, 9),
        voltage=f"{field(10, 11)}.{field(18, 19)}",
        parity_1=field(12, 12),
        field_13=field(13, 13),
        quantity_1=field(14, 17),
        parity_2=field(20, 20),
        field_21=field(21, 21),
        quantity_2=field(22, 25),
        quantity_3=field(26, 29),
        quantity_4=field(30, 33),
        error_code_hex=f"{raw[33]:02X}",
        document_xor_hex=f"{document_xor:02X}",
        document_xor_matches=raw[33] == document_xor,
    )
