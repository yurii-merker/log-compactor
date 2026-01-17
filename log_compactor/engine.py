import logging
from typing import List, Generator

from .exceptions import LogCompactorError
from .models import LogEntry, LogGroup
from .parser import LogParser

logger = logging.getLogger(__name__)


class CompactorEngine:
    """Core logic for managing the sliding window and log aggregation."""

    def __init__(self, dedup_window: int, error_threshold: int):
        self.dedup_window = dedup_window
        self.error_threshold = error_threshold
        self.parser = LogParser()
        self.active_groups: List[LogGroup] = []

    def process_file(self, file_path: str) -> Generator[str, None, None]:
        logger.info("Starting log compaction for file: %s", file_path)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        entry = self.parser.parse(line)
                        yield from self._handle_entry(entry)
                    except LogCompactorError as e:
                        logger.warning("Skipping line %d: %s", line_num, e)
                        continue

            yield from self._flush_remaining()
        except FileNotFoundError:
            logger.error("File not found: %s", file_path)
            return

    def _handle_entry(self, entry: LogEntry) -> Generator[str, None, None]:
        while self.active_groups and self._is_outside_window(
            entry, self.active_groups[0]
        ):
            yield self.active_groups.pop(0).format_summary()

        for group in self.active_groups:
            if group.is_compatible(entry, self.dedup_window):
                old_level = group.level
                group.update(entry, self.error_threshold)

                if old_level == "ERROR" and group.level == "CRITICAL":
                    logger.info(
                        "Escalated log group starting at %s to CRITICAL",
                        group.start_timestamp,
                    )
                return

        new_group = LogGroup(entry)
        if new_group.level == "ERROR" and self.error_threshold <= 1:
            new_group.level = "CRITICAL"
            new_group.is_escalated = True

        self.active_groups.append(new_group)

    def _is_outside_window(self, entry: LogEntry, group: LogGroup) -> bool:
        return (
            entry.timestamp - group.start_timestamp
        ).total_seconds() > self.dedup_window

    def _flush_remaining(self) -> Generator[str, None, None]:
        logger.debug("Flushing %d remaining active groups", len(self.active_groups))
        while self.active_groups:
            yield self.active_groups.pop(0).format_summary()
