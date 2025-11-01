from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure src/ is on sys.path to import the package without installation
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
for p in (PROJECT_ROOT, SRC_PATH):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

import dlt  # noqa: E402
from eml_to_database.config import AppConfig  # noqa: E402
from sources.eml_source import eml_source  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run EML headers pipeline to DuckDB using dlt")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(PROJECT_ROOT / "config/eml_config.yaml"),
        help="Path to YAML config file",
    )
    args = parser.parse_args()

    cfg = AppConfig.load(args.config)

    pipeline = dlt.pipeline(
        pipeline_name="eml_to_duckdb",  # standard pipeline name
        destination="duckdb",
        dataset_name=cfg.duckdb.dataset,
        dev_mode=False,
    )

    source = eml_source(str(cfg.eml.source_dir), cfg.eml.headers)

    # Configure duckdb destination path at run time
    try:
        from dlt.destinations import duckdb as duckdb_destination  # type: ignore
    except Exception:  # pragma: no cover
        duckdb_destination = None

    if duckdb_destination is not None:
        info = pipeline.run(source, destination=duckdb_destination(str(cfg.duckdb.database)))
    else:
        info = pipeline.run(source)

    print("Load completed:")
    print(info)


if __name__ == "__main__":
    main()
