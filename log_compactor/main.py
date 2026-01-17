from typing import Generator

from log_compactor.engine import CompactorEngine


def compact_logs(
    file_path: str,
    dedup_window_seconds: int,
    error_threshold: int,
) -> Generator[str, None, None]:
    """
    Entry point for log compaction.

    Reads logs from file_path and returns a generator of compacted log strings.
    """
    engine = CompactorEngine(
        dedup_window=dedup_window_seconds, error_threshold=error_threshold
    )
    return engine.process_file(file_path)
