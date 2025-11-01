from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping

from fast_mail_parser import parse_email, ParseError  # type: ignore

def _headers_lowercase_map(headers: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a mapping of header names lowercased to their values."""
    return {k.lower(): v for k, v in headers.items()}

def parse_eml_headers(path: Path) -> Dict | None:
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
        all_headers = _headers_lowercase_map(email_obj.headers)
    except ParseError:
        return None

    custom_headers = {
        k: v for k, v in all_headers.items() if k.startswith("x-")
    }
    standard_headers = {
        k: v for k, v in all_headers.items() if k not in custom_headers
    }

    return {
        **standard_headers,
        "custom_headers": [custom_headers],
    }
