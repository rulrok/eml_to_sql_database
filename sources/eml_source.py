from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import dlt

# We import the parser implementation from the src package
from eml_to_database.parser import parse_eml_headers  # type: ignore


@dlt.source
def eml_source(eml_dir: str, headers: List[str]):
    """dlt source that yields a messages resource with headers-only records."""
    eml_dir_path = Path(eml_dir)

    @dlt.resource(name="messages", write_disposition="append", primary_key="id")
    def messages() -> Iterable[dict]:
        for path in eml_dir_path.rglob("*.eml"):
            yield parse_eml_headers(path, headers)

    return (messages,)
