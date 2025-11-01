from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import re
import uuid
from email import policy
from email.parser import BytesParser


def parse_eml_headers(path: Path, headers_to_extract: List[str]) -> Dict:
    """Parse only headers from an EML file and return a flat record.

    Produces a stable id (Message-ID if present, else uuid4) and includes
    selected headers as snake_case columns in addition to the full headers map.
    """
    path = Path(path)
    with path.open("rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)

    all_headers = {k: v for (k, v) in msg.items()}
    headers_lc = {k.lower(): v for k, v in all_headers.items()}

    def hget(name: str) -> str | None:
        return headers_lc.get(name.lower())

    message_id = hget("Message-ID") or hget("Message-Id") or str(uuid.uuid4())

    record: Dict = {
        "id": message_id,
        "path": str(path.resolve()),
        "headers": all_headers,
    }
    for name in headers_to_extract:
        value = hget(name)
        key = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
        record[key] = value

    return record
