import datetime
from dataclasses import dataclass
from typing import Dict, Final


@dataclass(frozen=True)
class LogEntry:
    """Represents a single, normalized log event."""

    timestamp: datetime.datetime
    level: str
    fields: Dict[str, str]

    @property
    def identity_signature(self) -> str:
        sorted_fields = sorted(self.fields.items())
        field_str = " ".join(f"{k}={v}" for k, v in sorted_fields)
        return f"{self.level}|{field_str}"

    @property
    def error_escalation_signature(self) -> str:
        sorted_fields = sorted(self.fields.items())
        return " ".join(f"{k}={v}" for k, v in sorted_fields)


class LogGroup:
    """Manages a collection of log entries grouped for compaction."""

    TIME_ONLY_FORMAT: Final[str] = "%H:%M:%S"

    def __init__(self, entry: LogEntry):
        self.start_timestamp: datetime.datetime = entry.timestamp
        self.end_timestamp: datetime.datetime = entry.timestamp
        self.level: str = entry.level
        self.fields: Dict[str, str] = entry.fields
        self.count: int = 1
        self.is_escalated: bool = False

    def is_compatible(self, entry: LogEntry, window_seconds: int) -> bool:
        time_diff = (entry.timestamp - self.start_timestamp).total_seconds()
        if time_diff > window_seconds:
            return False

        if self.level == "CRITICAL" and entry.level == "ERROR":
            return self.fields == entry.fields

        return self.level == entry.level and self.fields == entry.fields

    def update(self, entry: LogEntry, error_threshold: int) -> None:
        self.end_timestamp = entry.timestamp
        self.count += 1

        if self.level == "ERROR" and self.count >= error_threshold:
            self.level = "CRITICAL"
            self.is_escalated = True

    def format_summary(self) -> str:
        timestamp_part = self._format_timestamp_range()
        sorted_fields = " ".join(f"{k}={v}" for k, v in sorted(self.fields.items()))
        count_part = f" (x{self.count})" if self.count > 1 else ""

        return f"{timestamp_part} {self.level} {sorted_fields}{count_part}"

    def _format_timestamp_range(self) -> str:
        start_iso = self.start_timestamp.isoformat()
        if self.start_timestamp == self.end_timestamp:
            return start_iso

        if self.start_timestamp.date() == self.end_timestamp.date():
            end_str = self.end_timestamp.strftime(self.TIME_ONLY_FORMAT)
        else:
            end_str = self.end_timestamp.isoformat()

        return f"{start_iso}~{end_str}"
