import logging

from log_compactor.main import compact_logs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


def main():
    for compacted_line in compact_logs("sample_logs.txt", 10, 2):
        print(compacted_line)


if __name__ == "__main__":
    main()
