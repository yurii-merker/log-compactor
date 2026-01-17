class LogCompactorError(Exception):
    """Base exception for all log compactor related errors."""


class MalformedLineError(LogCompactorError):
    """Raised when a log line does not conform to the expected format."""


class FieldConflictError(LogCompactorError):
    """Raised when mutually exclusive fields (e.g., user and user_id) conflict."""
