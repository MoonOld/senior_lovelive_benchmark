from __future__ import annotations

import re
import unicodedata
from urllib.parse import urljoin


SPACE_RE = re.compile(r"\s+")


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return SPACE_RE.sub(" ", value).strip()


def normalize_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", clean_text(value)).casefold()
    return re.sub(r"[\W_]+", " ", text).strip()


def absolute_url(base_url: str, href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(base_url, href)


def first_match(pattern: str, text: str, group: int = 1) -> str | None:
    match = re.search(pattern, text)
    if not match:
        return None
    return clean_text(match.group(group))


def unique_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result
