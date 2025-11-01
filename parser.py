from __future__ import annotations
import dateparser
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, TypedDict, cast

from fast_mail_parser import parse_email, ParseError  # type: ignore


# Use functional TypedDict to allow hyphenated keys like "message-id" and "custom-headers".
EmailRecord = TypedDict(
    "EmailRecord",
    {
        "message-id": str | None,
        "date": datetime | None,
        "subject": str | None,
        "text": str,
        "text_html": str,
        "headers": list[Dict[str, Any]],
        "custom-headers": list[Dict[str, Any]],
    },
    total=False,
)

def _headers_lowercase_map(headers: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a mapping of header names lowercased to their values."""
    return {k.lower(): v for k, v in headers.items()}

def _dict_to_header_entries(headers_dict: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Convert a dictionary to a list of Header/Value entries."""
    return [{"Header": k, "Value": v} for k, v in headers_dict.items()]

def parse_eml(path: Path) -> EmailRecord | None:
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

    message_id = standard_headers.get("message-id")

    return cast(
        EmailRecord,
        {
            "message-id": message_id,
            "date": dateparser.parse(email_obj.date),
            "subject": email_obj.subject,
            "text": "\n".join(email_obj.text_plain),
            "text_html": "<br>".join(email_obj.text_html),
            "headers": _dict_to_header_entries(standard_headers),
            "custom-headers": _dict_to_header_entries(custom_headers),
        },
    )
