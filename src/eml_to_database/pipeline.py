from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Dict

import dlt

from .config import AppConfig
from .parser import parse_eml_headers


@dlt.source
def eml_source(eml_dir: str, headers: List[str]):
    eml_dir_path = Path(eml_dir)

    @dlt.resource(name="messages", write_disposition="merge", primary_key="MESSAGE-ID")
    def messages() -> Iterable[dict]:
        for path in eml_dir_path.rglob("*.eml"):
            result = parse_eml_headers(path)
            if result:
                yield result

    return (messages,)


def load_from_dir(config_path: Path | str) -> object:
    cfg = AppConfig.load(config_path)

    pipeline = dlt.pipeline(
        pipeline_name="eml_to_duckdb",
        destination="duckdb",
        dataset_name=cfg.duckdb.dataset
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
        # Fallback: rely on environment/secret configuration
        info = pipeline.run(source)
    return info
