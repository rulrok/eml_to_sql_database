from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, TypedDict, cast

from fast_mail_parser import parse_email, ParseError  # type: ignore


class EmailRecord(TypedDict, total=False):
        """Typed shape for the parsed EML headers record.

        Notes:
        - The returned dict includes all standard headers as flat string keys
            (for example: "subject", "to", "date", "message-id", etc.).
        - In addition, it always contains a "custom_headers" key that groups any
            vendor-specific headers (those starting with "x-") as a single-item list
            containing a dict of those headers. This mirrors the runtime structure
            produced by ``parse_eml_headers``.
        - Extra standard header keys are allowed at runtime; they are not explicitly
            enumerated here and will be typed as ``str -> Any`` by type checkers.
        """

        # A single-item list containing a mapping of all custom (x-*) headers.
        custom_headers: list[Dict[str, Any]]

def _headers_lowercase_map(headers: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a mapping of header names lowercased to their values."""
    return {k.lower(): v for k, v in headers.items()}

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

    return cast(EmailRecord, {
        **standard_headers,
        "custom_headers": [custom_headers],
    })
