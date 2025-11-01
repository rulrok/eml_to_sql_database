from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Dict

import dlt

# We import the parser implementation from the src package
from eml_to_database.parser import parse_eml_headers  # type: ignore


@dlt.source
def eml_source(eml_dir: str, headers: List[str]):
    """dlt source that yields a messages resource with headers-only records."""
    eml_dir_path = Path(eml_dir)

    def _sanitize(h: str) -> str:
        import re as _re
        return _re.sub(r"[^0-9a-zA-Z]+", "_", h).strip("_").lower()

    cols: Dict[str, Dict[str, str]] = {
        "id": {"data_type": "text"},
        "path": {"data_type": "text"},
        "headers": {"data_type": "json"},
    }
    cols.update({
        _sanitize(h): {"data_type": "text"}
        for h in headers
    })

    @dlt.resource(name="messages", write_disposition="append", primary_key="id", columns=cols)
    def messages() -> Iterable[dict]:
        for path in eml_dir_path.rglob("*.eml"):
            yield parse_eml_headers(path, headers)

    return (messages,)
