import re
from pathlib import Path
from argparse import ArgumentParser


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("log_file", type=Path, help="Path to the log file to check")
    options = parser.parse_args()

    warning_pattern = re.compile(r"\bwarning\b", re.IGNORECASE)
    error_pattern = re.compile(r"\berror\b", re.IGNORECASE)

    warnings: dict[int, str] = {}
    errors: dict[int, str] = {}

    with open(options.log_file, "r") as log_file:
        for idx, line in enumerate(log_file):
            if warning_pattern.search(line):
                warnings[idx + 1] = line.strip()
            if error_pattern.search(line):
                errors[idx + 1] = line.strip()

    if warnings or errors:
        print(f"Issues found in log file: {log_file}")
        if warnings:
            print("\nWarning previews:")
            for line_num, line in warnings.items():
                print(f"Line {line_num}: {line[:100]}")
        if errors:
            print("\nError previews:")
            for line_num, line in errors.items():
                print(f"Line {line_num}: {line[:100]}")


if __name__ == "__main__":
    main()
