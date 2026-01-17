import datetime
import logging
import re
from typing import Dict, Final

from .exceptions import MalformedLineError, FieldConflictError
from .models import LogEntry

logger = logging.getLogger(__name__)


class LogParser:
    """Responsible for parsing raw text lines into normalized LogEntry objects."""

    FIELD_PATTERN: Final[re.Pattern] = re.compile(r"(\S+)=(\S+)")
    ERROR_CODE_RANGE: Final[range] = range(500, 600)

    def parse(self, line: str) -> LogEntry:
        parts = line.strip().split(" ", 2)
        if len(parts) < 3:
            raise MalformedLineError(f"Line too short: {line.strip()}")

        timestamp_str, level, fields_str = parts
        timestamp = self._parse_timestamp(timestamp_str)

        if not level.isupper():
            raise MalformedLineError(f"Level not uppercase: {level}")

        raw_fields = self._parse_fields(fields_str)
        normalized_fields = self._normalize_fields(raw_fields)

        final_level = self._determine_level(level, normalized_fields)

        logger.debug(
            f"Parsed line into LogEntry: {timestamp}, {final_level}, {normalized_fields}"
        )
        return LogEntry(
            timestamp=timestamp, level=final_level, fields=normalized_fields
        )

    def _parse_timestamp(self, ts_str: str) -> datetime.datetime:
        try:
            return datetime.datetime.fromisoformat(ts_str)
        except ValueError:
            raise MalformedLineError(f"Invalid ISO-8601 timestamp: {ts_str}")

    def _parse_fields(self, fields_str: str) -> Dict[str, str]:
        matches = self.FIELD_PATTERN.findall(fields_str)
        if not matches:
            raise MalformedLineError(f"No valid fields in: {fields_str}")
        return dict(matches)

    def _normalize_fields(self, fields: Dict[str, str]) -> Dict[str, str]:
        user = fields.pop("user", None)
        user_id = fields.pop("user_id", None)

        if user is not None and user_id is not None and user != user_id:
            raise FieldConflictError(f"User conflict: user={user}, user_id={user_id}")

        final_user = user or user_id
        if final_user:
            fields["user"] = final_user

        return fields

    def _determine_level(self, original_level: str, fields: Dict[str, str]) -> str:
        if "code" in fields:
            try:
                if int(fields["code"]) in self.ERROR_CODE_RANGE:
                    return "ERROR"
            except ValueError:
                pass
        return original_level
