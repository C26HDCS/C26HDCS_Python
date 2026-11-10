"""Transport-independent receive pipeline."""

from dataclasses import dataclass

from .protocol import FrameCandidate, FrameDecoder, FrameError, ParsedFrame, parse_frame
from .storage import FrameStore


@dataclass(frozen=True)
class Receipt:
    row_id: int
    raw_hex: str
    parsed: ParsedFrame | None
    error: str | None
    mapping_error: str | None = None


class Ingestor:
    def __init__(self, store: FrameStore, source: str) -> None:
        self._store = store
        self._source = source
        self._decoder = FrameDecoder()

    def feed(self, chunk: bytes) -> list[Receipt]:
        return [self._record(candidate) for candidate in self._decoder.feed(chunk)]

    def finish(self) -> list[Receipt]:
        return [self._record(candidate) for candidate in self._decoder.finish()]

    def _record(self, candidate: FrameCandidate) -> Receipt:
        parsed = None
        error = candidate.framing_error
        if error is None:
            try:
                parsed = parse_frame(candidate.raw)
            except FrameError as exc:
                error = str(exc)

        row_id = self._store.save(
            source=self._source,
            raw=candidate.raw,
            parsed=parsed,
            error=error,
        )
        return Receipt(row_id, candidate.raw.hex().upper(), parsed, error, self._store.last_mapping_error)
