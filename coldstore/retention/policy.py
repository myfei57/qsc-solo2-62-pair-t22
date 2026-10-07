"""Per category retention windows measured in seconds.

Windows are measured in time, not in event counts: a count based window can
be flushed empty by a burst of events exactly when the history is needed,
while a time window guarantees how far back the log can always be queried.
"""

from __future__ import annotations

import json

from coldstore.store.store import Store

WINDOWS_KEY = "retention:windows"
MAX_WINDOW_SECONDS = 10 * 365 * 24 * 3600
MAX_CATEGORY_LENGTH = 64


def validate_category(category: str) -> str:
    if not category:
        raise ValueError("category is required")
    if len(category) > MAX_CATEGORY_LENGTH:
        raise ValueError("category is too long")
    return category


def validate_window(seconds: int) -> int:
    if seconds <= 0:
        raise ValueError("window seconds must be positive")
    if seconds > MAX_WINDOW_SECONDS:
        raise ValueError("window seconds exceed the supported range")
    return int(seconds)


def load_windows(store: Store) -> dict[str, int]:
    """Return the configured windows, tolerating a corrupt document."""

    raw, present = store.get(WINDOWS_KEY)
    if not present:
        return {}
    try:
        payload = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(payload, dict):
        return {}
    windows: dict[str, int] = {}
    for key, value in payload.items():
        try:
            seconds = int(value)
        except (TypeError, ValueError):
            continue
        if seconds > 0:
            windows[str(key)] = seconds
    return windows


def save_windows(store: Store, windows: dict[str, int]) -> None:
    store.put(WINDOWS_KEY, json.dumps(windows, ensure_ascii=False, sort_keys=True))
