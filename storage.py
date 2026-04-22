"""
storage.py — Atomic persistence for profiles, cookies, groups, and topics.
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

_SCHEMA_VERSION = 1


# ── Helpers ────────────────────────────────────────────────────────────────────

def _atomic_write_json(path: Path, data: object) -> None:
    """Write JSON atomically via temp-file + rename."""
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent, prefix=f".{path.stem}_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _safe_read_json(path: Path, default: object) -> object:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to read %s: %s — using default.", path, exc)
        return default


# ── ProfileStore ───────────────────────────────────────────────────────────────

class ProfileStore:
    """
    Manages profiles.json with atomic writes.

    Schema:
        {"version": 1, "profiles": {"instagram": ["https://..."], ...}}
    """

    def __init__(self, cfg: "Config") -> None:
        self._path      = cfg.profiles_file
        self._platforms = list(cfg.platforms.keys())
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict:
        raw = _safe_read_json(self._path, {})
        if not isinstance(raw, dict) or "profiles" not in raw:
            return self._empty()
        for p in self._platforms:
            raw["profiles"].setdefault(p, [])
        return raw

    def _empty(self) -> dict:
        return {"version": _SCHEMA_VERSION, "profiles": {p: [] for p in self._platforms}}

    def _save(self, data: dict) -> None:
        _atomic_write_json(self._path, data)

    def all(self) -> dict[str, list[str]]:
        return self._load()["profiles"]

    def get(self, platform: str) -> list[str]:
        return self._load()["profiles"].get(platform, [])

    def add(self, platform: str, url: str) -> bool:
        data   = self._load()
        bucket = data["profiles"].setdefault(platform, [])
        if url in bucket:
            return False
        bucket.append(url)
        self._save(data)
        logger.info("Profile added: %s → %s", url, platform)
        return True

    def add_bulk(self, platform: str, urls: list[str]) -> int:
        data   = self._load()
        bucket = data["profiles"].setdefault(platform, [])
        added  = sum(1 for u in urls if u and u not in bucket and not bucket.append(u))
        if added:
            self._save(data)
        return added

    def remove(self, platform: str, url: str) -> bool:
        data   = self._load()
        bucket = data["profiles"].get(platform, [])
        if url not in bucket:
            return False
        bucket.remove(url)
        self._save(data)
        return True

    def clear(self, platform: str) -> int:
        data  = self._load()
        count = len(data["profiles"].get(platform, []))
        data["profiles"][platform] = []
        self._save(data)
        return count

    def total_count(self) -> int:
        return sum(len(v) for v in self.all().values())


# ── CookieStore ────────────────────────────────────────────────────────────────

class CookieStore:
    """Manages uploaded Netscape-format cookie files."""

    VALID_NAMES: frozenset[str] = frozenset({
        "instagram.com_cookies.txt",
        "tiktok.com_cookies.txt",
        "facebook.com_cookies.txt",
        "x.com_cookies.txt",
    })

    def __init__(self, cfg: "Config") -> None:
        self._dir = cfg.cookies_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def is_valid_name(self, name: str) -> bool:
        return name in self.VALID_NAMES

    def save(self, name: str, data: bytes) -> Path:
        dest = self._dir / name
        fd, tmp = tempfile.mkstemp(dir=self._dir, prefix=".cookie_")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            os.replace(tmp, dest)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        logger.info("Cookie saved: %s (%d bytes)", name, len(data))
        return dest

    def path_for(self, cookie_file: str) -> Path:
        return self._dir / cookie_file

    def exists(self, cookie_file: str) -> bool:
        return (self._dir / cookie_file).exists()

    def list_all(self) -> list[tuple[str, int]]:
        """Returns [(name, size_bytes), ...] sorted by name."""
        result = []
        for f in sorted(self._dir.iterdir()):
            if f.is_file() and f.suffix == ".txt":
                try:
                    result.append((f.name, f.stat().st_size))
                except OSError:
                    pass
        return result


# ── GroupStore ─────────────────────────────────────────────────────────────────

class GroupStore:
    """
    Manages the whitelist of Telegram group/supergroup IDs
    that the bot is allowed to operate in.
    """

    def __init__(self, data_dir: Path) -> None:
        self._path = data_dir / "groups.json"
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> list[int]:
        raw = _safe_read_json(self._path, [])
        if not isinstance(raw, list):
            return []
        try:
            return [int(x) for x in raw]
        except (ValueError, TypeError):
            return []

    def _save(self, ids: list[int]) -> None:
        _atomic_write_json(self._path, ids)

    def allow(self, group_id: int) -> bool:
        """Add group to whitelist. Returns True if newly added."""
        ids = self._load()
        if group_id in ids:
            return False
        ids.append(group_id)
        self._save(ids)
        logger.info("Group allowed: %d", group_id)
        return True

    def deny(self, group_id: int) -> bool:
        """Remove group from whitelist. Returns True if was present."""
        ids = self._load()
        if group_id not in ids:
            return False
        ids.remove(group_id)
        self._save(ids)
        logger.info("Group removed: %d", group_id)
        return True

    def is_allowed(self, chat_id: int) -> bool:
        return chat_id in self._load()

    def list_all(self) -> list[int]:
        return self._load()


# ── TopicStore ────────────────────────────────────────────────────────────────

class TopicStore:
    """
    Stores Telegram forum topic thread IDs per group + platform + username.

    Schema:
        {"<group_id>": {"<platform>:<username>": <thread_id>, ...}, ...}
    """

    def __init__(self, data_dir: Path) -> None:
        self._path = data_dir / "topics.json"
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, dict[str, int]]:
        raw = _safe_read_json(self._path, {})
        return raw if isinstance(raw, dict) else {}

    def _save(self, data: dict) -> None:
        _atomic_write_json(self._path, data)

    @staticmethod
    def _key(platform: str, username: str) -> str:
        return f"{platform}:{username}"

    def get(self, group_id: int, platform: str, username: str) -> int | None:
        data  = self._load()
        group = data.get(str(group_id), {})
        val   = group.get(self._key(platform, username))
        return int(val) if val is not None else None

    def set(self, group_id: int, platform: str, username: str, thread_id: int) -> None:
        data = self._load()
        gkey = str(group_id)
        data.setdefault(gkey, {})[self._key(platform, username)] = thread_id
        self._save(data)
        logger.debug("Topic stored: group=%d %s:%s → thread=%d", group_id, platform, username, thread_id)
