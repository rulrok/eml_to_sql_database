from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import load_from_dir


def main():
    parser = argparse.ArgumentParser(description="Load EML headers into DuckDB using dlt")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/eml_config.yaml"),
        help="Path to YAML config file",
    )
    args = parser.parse_args()

    info = load_from_dir(args.config)
    print("Load completed:")
    print(info)


if __name__ == "__main__":
    main()
