from datetime import datetime, timezone
from typing import Any, Dict

DATE_PART_MARKER = "__date_part__"
MONTH_LABELS = [
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]


def parse_date_part_key(key: str | None) -> Dict[str, str] | None:
    if not key or not isinstance(key, str):
        return None
    index = key.rfind(DATE_PART_MARKER)
    if index == -1:
        return None
    field_key = key[:index]
    part = key[index + len(DATE_PART_MARKER) :]
    if not field_key or part not in {"year", "month", "day"}:
        return None
    return {"field_key": field_key, "part": part}


def parse_date_input(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if "." in text:
        parts = text.split(".")
        if len(parts) == 3:
            day, month, year = parts
            try:
                return datetime.fromisoformat(f"{year}-{month}-{day}T00:00:00+00:00")
            except ValueError:
                pass
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        try:
            return datetime.fromisoformat(f"{text}T00:00:00+00:00")
        except ValueError:
            return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def resolve_date_part_value(value: Any, part: str) -> str | None:
    parsed = parse_date_input(value)
    if not parsed:
        return None
    if part == "year":
        return str(parsed.year)
    if part == "month":
        month_index = parsed.month - 1
        numeric = f"{parsed.month:02d}"
        label = MONTH_LABELS[month_index] if 0 <= month_index < len(MONTH_LABELS) else ""
        return f"{numeric} — {label}" if label else numeric
    if part == "day":
        return f"{parsed.day:02d}"
    return None
