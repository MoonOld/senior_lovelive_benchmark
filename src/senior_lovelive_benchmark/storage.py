from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, TypeAdapter

T = TypeVar("T", bound=BaseModel)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, records: Iterable[BaseModel]) -> int:
    ensure_parent(path)
    count = 0
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as file:
        temp_path = Path(file.name)
        try:
            for record in records:
                file.write(json.dumps(record.model_dump(mode="json"), ensure_ascii=False))
                file.write("\n")
                count += 1
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
    os.replace(temp_path, path)
    return count


def append_jsonl(path: Path, records: Iterable[BaseModel]) -> int:
    ensure_parent(path)
    count = 0
    with path.open("a", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record.model_dump(mode="json"), ensure_ascii=False))
            file.write("\n")
            count += 1
    return count


def read_jsonl(path: Path, model: type[T]) -> list[T]:
    if not path.exists():
        return []
    adapter = TypeAdapter(model)
    records: list[T] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if stripped:
                records.append(adapter.validate_python(json.loads(stripped)))
    return records


def dedupe_records(records: Iterable[T], key: Callable[[T], str]) -> list[T]:
    seen: set[str] = set()
    deduped: list[T] = []
    for record in records:
        record_key = key(record)
        if record_key in seen:
            continue
        seen.add(record_key)
        deduped.append(record)
    return deduped


def merge_jsonl(path: Path, records: Iterable[T], key: Callable[[T], str], model: type[T]) -> int:
    existing = read_jsonl(path, model)
    # Fresh crawls should replace stale parsed fields for the same source id.
    merged_by_key = {key(record): record for record in existing}
    for record in records:
        merged_by_key[key(record)] = record
    merged = list(merged_by_key.values())
    return write_jsonl(path, merged)
