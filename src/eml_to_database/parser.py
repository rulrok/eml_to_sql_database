from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping
import re
import uuid

from fast_mail_parser import parse_email, ParseError  # type: ignore


def _build_lookup(headers: Mapping[str, Any]) -> Dict[str, Any]:
    """Build a case-insensitive lookup for header names with dash/underscore variants."""
    lu: Dict[str, Any] = {}
    for k, v in headers.items():
        k_l = str(k).lower()
        lu[k_l] = v
        lu[k_l.replace("-", "_")] = v
    return lu


def parse_eml_headers(path: Path, headers_to_extract: List[str]) -> Dict:
    """Parse headers from an EML file using fast_mail_parser and return a flat record.

    Contract:
    - Inputs: path to .eml file, list of header names to extract
    - Output: dict with keys: id, path, headers (full map), and one snake_case key per requested header
    - id: Message-ID if present, else a generated uuid4 string
    """
    path = Path(path)

    # fast_mail_parser expects a text string; read as UTF-8 with replacement to be robust
    try:
        payload = path.read_text(encoding="utf-8", errors="replace")
        email_obj = parse_email(payload)
        all_headers_raw = getattr(email_obj, "headers", {}) or {}
        all_headers = dict(all_headers_raw) if isinstance(all_headers_raw, Mapping) else {}
    except ParseError:
        # On parse failure, return minimal record with generated id and no headers
        all_headers = {}
        email_obj = None  # type: ignore

    lookup = _build_lookup(all_headers)

    def hget(name: str) -> Any | None:
        n = name.strip()
        candidates = [
            n,
            n.lower(),
            n.replace("_", "-").lower(),
            n.replace("-", "_").lower(),
            "message-id" if n.lower() in {"message_id", "message-id", "messageid", "message id"} else n.lower(),
        ]
        for c in candidates:
            if c in lookup:
                return lookup[c]
        return None

    message_id = hget("Message-ID") or hget("Message_Id") or str(uuid.uuid4())

    record: Dict[str, Any] = {
        "id": message_id,
        "path": str(path.resolve()),
        "headers": all_headers,
    }

    for name in headers_to_extract:
        value = hget(name)
        key = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
        record[key] = value

    return record
