# Log Compactor

A robust, memory-efficient Python utility for processing, deduplicating, and summarizing high-volume log files.

## Overview

This tool reads chronologically ordered log files and produces a compact, human-readable summary. It is designed to handle arbitrarily large files by using a streaming **Sliding Window** approach, ensuring memory usage is bound by the window size rather than the file size.

### Key Features

* **Deduplication**: Groups identical log entries within a configurable time window.
* **Error Escalation**: automatically promotes `ERROR` groups to `CRITICAL` if they exceed a specific frequency threshold.
* **Enrichment**: Detects HTTP-style error codes (`500`-`599`) and overrides the log level to `ERROR` regardless of the original tag.
* **Normalization**: Handles field aliases (e.g., standardizing `user_id` to `user`) and validates data integrity.
* **Zero External Dependencies**: Built entirely with the Python Standard Library for the core runtime.

---

## Project Structure

```text
.
├── log_compactor/          # Core package
│   ├── __init__.py
│   ├── engine.py           # Window logic & aggregation
│   ├── exceptions.py       # Custom exception classes
│   ├── main.py             # Public API entry point
│   ├── models.py           # Data classes (LogEntry, LogGroup)
│   └── parser.py           # Text parsing & normalization
├── tests/                  # Test suite
│   └── test_log_compactor.py
├── README.md               # Project documentation
├── requirements.txt    # Testing dependencies
└── entrypoint.py        # Execution script