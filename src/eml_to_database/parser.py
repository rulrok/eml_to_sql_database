from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping
import re
import uuid


def _decode_with_eml_parser(raw_bytes: bytes) -> Dict[str, Any]:
    """Decode EML bytes using the eml_parser package.

    Tries the class-based API first and falls back to the functional API
    depending on the installed version.
    """
    try:
        # Newer API
        from eml_parser import EmlParser  # type: ignore

        parser = EmlParser(
            include_attachment_data=False,
            include_raw_body=False,
            parse_attachments=False,
        )
        return parser.decode_email_bytes(raw_bytes)
    except Exception:
        # Older API fallback
        from eml_parser import eml_parser as eml_fn  # type: ignore

        return eml_fn.decode_email_bytes(raw_bytes)


def _get_headers_map(parsed: Mapping[str, Any]) -> Dict[str, Any]:
    """Return headers mapping from parsed object regardless of key naming."""
    headers: Dict[str, Any] = {}
    if isinstance(parsed, Mapping):
        # Common keys in eml_parser outputs
        for key in ("header", "headers"):
            value = parsed.get(key)  # type: ignore[index]
            if isinstance(value, Mapping):
                headers = dict(value)  # normalize to plain dict
                break
    return headers


def _build_lookup(headers: Mapping[str, Any]) -> Dict[str, Any]:
    """Build a case-insensitive lookup for header names with dash/underscore variants."""
    lu: Dict[str, Any] = {}
    for k, v in headers.items():
        k_l = str(k).lower()
        lu[k_l] = v
        lu[k_l.replace("-", "_")] = v
    return lu


def parse_eml_headers(path: Path, headers_to_extract: List[str]) -> Dict:
    """Parse headers from an EML file using eml_parser and return a flat record.

    Contract:
    - Inputs: path to .eml file, list of header names to extract
    - Output: dict with keys: id, path, headers (full map), and one snake_case key per requested header
    - id: Message-ID if present, else a generated uuid4 string
    """
    path = Path(path)
    raw = path.read_bytes()

    parsed = _decode_with_eml_parser(raw)
    all_headers = _get_headers_map(parsed)
    lookup = _build_lookup(all_headers)

    def hget(name: str) -> Any | None:
        n = name.strip()
        # try exact lower, dash/underscore variants and common capitalizations
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
