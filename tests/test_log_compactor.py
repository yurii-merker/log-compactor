from pathlib import Path

import pytest

from log_compactor.exceptions import FieldConflictError
from log_compactor.main import compact_logs
from log_compactor.models import LogGroup
from log_compactor.parser import LogParser


class TestLogCompactor:

    @pytest.fixture
    def parser(self) -> LogParser:
        return LogParser()

    @pytest.fixture
    def temp_log_file(self, tmp_path: Path) -> Path:
        return tmp_path / "test_logs.txt"

    def test_field_normalization_user_alias(self, parser: LogParser):
        """Verify user_id is treated as an alias for user and normalized."""
        line = "2024-01-01T10:00:00 INFO user_id=alice action=login"
        entry = parser.parse(line)
        assert "user" in entry.fields
        assert entry.fields["user"] == "alice"
        assert "user_id" not in entry.fields

    def test_field_conflict_raises_error(self, parser: LogParser):
        """Verify conflicting user and user_id values cause a MalformedLineError."""
        line = "2024-01-01T10:00:00 INFO user=alice user_id=bob"
        with pytest.raises(FieldConflictError):
            parser.parse(line)

    def test_error_code_enrichment(self, parser: LogParser):
        """Verify code 500-599 overrides level to ERROR."""
        line = "2024-01-01T10:00:00 INFO action=retry code=503"
        entry = parser.parse(line)
        assert entry.level == "ERROR"

    def test_compaction_and_escalation(self, temp_log_file: Path):
        """Verify deduplication and escalation to CRITICAL in the generator."""
        content = [
            "2024-01-01T10:00:00 ERROR action=fail user=alice",
            "2024-01-01T10:00:01 ERROR action=fail user=alice",
            "2024-01-01T10:00:30 INFO action=other user=bob",
        ]
        temp_log_file.write_text("\n".join(content))

        results = list(compact_logs(str(temp_log_file), 10, 2))

        assert len(results) == 2
        assert "CRITICAL" in results[0]
        assert "(x2)" in results[0]
        assert "10:00:00~10:00:01" in results[0]
        assert "INFO" in results[1]
        assert "user=bob" in results[1]

    def test_malformed_lines_are_skipped(self, temp_log_file: Path):
        """Verify that junk lines do not crash the engine."""
        content = [
            "this is just garbage text",
            "2024-01-01T10:00:00 INFO user=alice action=login",
            "2024-01-01-NOT-A-DATE INFO user=bob",
        ]
        temp_log_file.write_text("\n".join(content))

        results = list(compact_logs(str(temp_log_file), 60, 5))
        assert len(results) == 1
        assert "user=alice" in results[0]

    def test_timestamp_range_formatting_cross_day(self, parser: LogParser):
        """Verify full ISO is used for end_timestamp if dates differ."""
        entry1 = parser.parse("2024-01-01T23:59:59 INFO action=tick")
        entry2 = parser.parse("2024-01-02T00:00:01 INFO action=tick")

        group = LogGroup(entry1)
        group.update(entry2, error_threshold=5)

        summary = group.format_summary()
        assert "2024-01-01T23:59:59~2024-01-02T00:00:01" in summary

    def test_stable_ordering(self, temp_log_file: Path):
        """Verify output maintains order of the first appearance."""
        content = [
            "2024-01-01T10:00:00 INFO type=A",
            "2024-01-01T10:00:01 INFO type=B",
            "2024-01-01T10:00:02 INFO type=A",
        ]
        temp_log_file.write_text("\n".join(content))

        results = list(compact_logs(str(temp_log_file), 60, 5))
        assert "type=A" in results[0]
        assert "type=B" in results[1]
