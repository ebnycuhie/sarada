"""
storage.py — Persistent storage for profiles, cookies, groups, and forum topics.

All stores use atomic write patterns (write-then-rename) to prevent corruption
on process crash mid-write.  Thread-safety is not required because the bot
runs single-threaded via python-telegram-bot's async event loop.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import Config

logger = logging.getLogger(__name__)

# ── Atomic write helper ───────────────────────────────────────────────────────

def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write *data* to *path* atomically using a temp-file + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp_")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _atomic_write_json(path: Path, obj: object) -> None:
    data = json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
    _atomic_write_bytes(path, data)


def _load_json(path: Path, default: object) -> object:
    try:
        return json.loads(path.read_text("utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


# ── ProfileStore ──────────────────────────────────────────────────────────────

class ProfileStore:
    """
    Persists per-platform profile URL lists to a single JSON file.

    Schema: { "instagram": ["url1", ...], "tiktok": [...], ... }
    """

    def __init__(self, cfg: "Config") -> None:
        self._path = cfg.profiles_file
        self._platforms = list(cfg.platforms.keys())
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, list[str]] = self._load()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _load(self) -> dict[str, list[str]]:
        raw = _load_json(self._path, {})
        if not isinstance(raw, dict):
            raw = {}
        result: dict[str, list[str]] = {}
        for plat in self._platforms:
            entries = raw.get(plat, [])
            result[plat] = [str(u) for u in entries if isinstance(u, str)]
        return result

    def _save(self) -> None:
        _atomic_write_json(self._path, self._data)

    # ── Public API ────────────────────────────────────────────────────────────

    def get(self, platform: str) -> list[str]:
        """Return the URL list for *platform* (never None)."""
        return list(self._data.get(platform, []))

    def all(self) -> dict[str, list[str]]:
        """Return a copy of all platform → URL lists."""
        return {p: list(urls) for p, urls in self._data.items()}

    def total_count(self) -> int:
        return sum(len(v) for v in self._data.values())

    def add(self, platform: str, url: str) -> bool:
        """Add *url* to *platform*. Returns True if added, False if duplicate."""
        lst = self._data.setdefault(platform, [])
        url = url.rstrip("/")
        if url in lst:
            return False
        lst.append(url)
        self._save()
        logger.info("Profile added: %s → %s", url, platform)
        return True

    def add_bulk(self, platform: str, urls: list[str]) -> int:
        """Add multiple URLs; returns count of newly added entries."""
        lst = self._data.setdefault(platform, [])
        existing = set(lst)
        added = 0
        for url in urls:
            url = url.rstrip("/")
            if url not in existing:
                lst.append(url)
                existing.add(url)
                added += 1
        if added:
            self._save()
        logger.info("Bulk import: %d added to %s", added, platform)
        return added

    def remove(self, platform: str, url: str) -> bool:
        """Remove *url* from *platform*. Returns True if removed."""
        url = url.rstrip("/")
        lst = self._data.get(platform, [])
        if url in lst:
            lst.remove(url)
            self._save()
            logger.info("Profile removed: %s from %s", url, platform)
            return True
        return False

    def clear(self, platform: str) -> int:
        """Clear all URLs for *platform*. Returns count cleared."""
        lst = self._data.get(platform, [])
        count = len(lst)
        self._data[platform] = []
        self._save()
        logger.info("Cleared %d profile(s) from %s", count, platform)
        return count


# ── CookieStore ───────────────────────────────────────────────────────────────

# Valid cookie filenames the bot accepts (maps to platforms)
_VALID_COOKIE_NAMES: frozenset[str] = frozenset({
    "instagram.com_cookies.txt",
    "tiktok.com_cookies.txt",
    "facebook.com_cookies.txt",
    "x.com_cookies.txt",
})


class CookieStore:
    """Manages per-platform Netscape cookie files in *cookies_dir*."""

    def __init__(self, cfg: "Config") -> None:
        self._dir = cfg.cookies_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    # ── Public API ────────────────────────────────────────────────────────────

    @staticmethod
    def is_valid_name(name: str) -> bool:
        return name in _VALID_COOKIE_NAMES

    def save(self, name: str, data: bytes) -> None:
        """Write *data* as cookie file *name*, atomically."""
        dest = self._dir / name
        _atomic_write_bytes(dest, data)
        logger.info("Cookie saved: %s (%d bytes)", name, len(data))

    def path_for(self, filename: str) -> Path:
        return self._dir / filename

    def list_all(self) -> list[tuple[str, int]]:
        """Return list of (filename, size_bytes) for all present cookie files."""
        result: list[tuple[str, int]] = []
        for name in _VALID_COOKIE_NAMES:
            p = self._dir / name
            if p.exists():
                result.append((name, p.stat().st_size))
        return sorted(result)


# ── GroupStore ────────────────────────────────────────────────────────────────

class GroupStore:
    """
    Persists the set of whitelisted Telegram group/supergroup chat IDs.

    Schema: { "allowed": [chat_id, ...] }
    """

    def __init__(self, data_dir: Path) -> None:
        self._path = data_dir / "groups.json"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._allowed: set[int] = self._load()

    def _load(self) -> set[int]:
        raw = _load_json(self._path, {"allowed": []})
        if not isinstance(raw, dict):
            return set()
        entries = raw.get("allowed", [])
        return {int(x) for x in entries if isinstance(x, (int, str))}

    def _save(self) -> None:
        _atomic_write_json(self._path, {"allowed": sorted(self._allowed)})

    # ── Public API ────────────────────────────────────────────────────────────

    def is_allowed(self, chat_id: int) -> bool:
        return chat_id in self._allowed

    def allow(self, chat_id: int) -> bool:
        """Whitelist *chat_id*. Returns True if newly added."""
        if chat_id in self._allowed:
            return False
        self._allowed.add(chat_id)
        self._save()
        logger.info("Group whitelisted: %d", chat_id)
        return True

    def deny(self, chat_id: int) -> bool:
        """Remove *chat_id* from whitelist. Returns True if removed."""
        if chat_id not in self._allowed:
            return False
        self._allowed.discard(chat_id)
        self._save()
        logger.info("Group removed: %d", chat_id)
        return True

    def list_all(self) -> list[int]:
        return sorted(self._allowed)


# ── TopicStore ────────────────────────────────────────────────────────────────

class TopicStore:
    """
    Persists forum topic thread IDs so repeated runs reuse the same topic.

    Schema: { "chat_id:platform:username": thread_id, ... }
    """

    def __init__(self, data_dir: Path) -> None:
        self._path = data_dir / "topics.json"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, int] = self._load()

    def _load(self) -> dict[str, int]:
        raw = _load_json(self._path, {})
        if not isinstance(raw, dict):
            return {}
        return {str(k): int(v) for k, v in raw.items() if isinstance(v, (int, float))}

    def _save(self) -> None:
        _atomic_write_json(self._path, self._data)

    @staticmethod
    def _key(chat_id: int, platform: str, username: str) -> str:
        return f"{chat_id}:{platform}:{username}"

    # ── Public API ────────────────────────────────────────────────────────────

    def get(self, chat_id: int, platform: str, username: str) -> int | None:
        return self._data.get(self._key(chat_id, platform, username))

    def set(self, chat_id: int, platform: str, username: str, thread_id: int) -> None:
        self._data[self._key(chat_id, platform, username)] = thread_id
        self._save()
