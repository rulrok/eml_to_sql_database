from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import yaml


@dataclass
class EmlConfig:
    source_dir: Path
    headers: List[str] = field(default_factory=list)


@dataclass
class DuckDBConfig:
    database: Path = Path("./data/eml.duckdb")
    dataset: str = "eml_data"


@dataclass
class AppConfig:
    eml: EmlConfig
    duckdb: DuckDBConfig

    @staticmethod
    def load(path: Path | str) -> "AppConfig":
        path = Path(path)
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        eml = data.get("eml", {})
        duck = data.get("duckdb", {})
        cfg = AppConfig(
            eml=EmlConfig(
                source_dir=Path(eml.get("source_dir", "./sample_data")).resolve(),
                headers=[str(h) for h in eml.get("headers")] if isinstance(eml.get("headers"), list) else [],
            ),
            duckdb=DuckDBConfig(
                database=Path(duck.get("database", "./data/eml.duckdb")).resolve(),
                dataset=str(duck.get("dataset", "eml_data")),
            ),
        )
        return cfg
