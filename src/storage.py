"""
storage.py — Thread-safe, atomic profile and cookie persistence.
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


class ProfileStore:
    """
    Manages profiles.json with atomic writes to prevent corruption.

    Schema:
    {
        "version": 1,
        "profiles": {
            "instagram": ["https://..."],
            ...
        }
    }
    """

    def __init__(self, cfg: "Config") -> None:
        self._path = cfg.profiles_file
        self._platforms = list(cfg.platforms.keys())
        self._path.parent.mkdir(parents=True, exist_ok=True)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _load_raw(self) -> dict:
        if not self._path.exists():
            return self._empty()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or "profiles" not in data:
                logger.warning("profiles.json schema invalid, resetting.")
                return self._empty()
            # Ensure all platform keys exist
            for p in self._platforms:
                data["profiles"].setdefault(p, [])
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed to read profiles.json: %s — resetting.", exc)
            return self._empty()

    def _empty(self) -> dict:
        return {
            "version": _SCHEMA_VERSION,
            "profiles": {p: [] for p in self._platforms},
        }

    def _save(self, data: dict) -> None:
        """Atomic write via temp file + rename."""
        parent = self._path.parent
        try:
            fd, tmp = tempfile.mkstemp(dir=parent, prefix=".profiles_", suffix=".json")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(data, fh, indent=2, ensure_ascii=False)
                os.replace(tmp, self._path)
            except Exception:
                os.unlink(tmp)
                raise
        except OSError as exc:
            logger.error("Failed to save profiles.json: %s", exc)
            raise

    # ── Public API ────────────────────────────────────────────────────────────

    def all(self) -> dict[str, list[str]]:
        """Return {platform: [url, ...]} mapping."""
        return self._load_raw()["profiles"]

    def get(self, platform: str) -> list[str]:
        return self._load_raw()["profiles"].get(platform, [])

    def add(self, platform: str, url: str) -> bool:
        """Add URL. Returns True if added, False if duplicate."""
        data = self._load_raw()
        bucket: list = data["profiles"].setdefault(platform, [])
        if url in bucket:
            return False
        bucket.append(url)
        self._save(data)
        logger.info("Added %s → %s", url, platform)
        return True

    def add_bulk(self, platform: str, urls: list[str]) -> int:
        """Add multiple URLs. Returns count of newly added."""
        data = self._load_raw()
        bucket: list = data["profiles"].setdefault(platform, [])
        added = 0
        for url in urls:
            if url and url not in bucket:
                bucket.append(url)
                added += 1
        if added:
            self._save(data)
            logger.info("Bulk-added %d URLs to %s", added, platform)
        return added

    def remove(self, platform: str, url: str) -> bool:
        """Remove URL. Returns True if removed, False if not found."""
        data = self._load_raw()
        bucket: list = data["profiles"].get(platform, [])
        if url not in bucket:
            return False
        bucket.remove(url)
        self._save(data)
        logger.info("Removed %s from %s", url, platform)
        return True

    def clear(self, platform: str) -> int:
        """Clear all URLs for a platform. Returns count cleared."""
        data = self._load_raw()
        count = len(data["profiles"].get(platform, []))
        data["profiles"][platform] = []
        self._save(data)
        logger.info("Cleared %d URL(s) from %s", count, platform)
        return count

    def total_count(self) -> int:
        return sum(len(v) for v in self.all().values())


class CookieStore:
    """Manages uploaded cookie files in the cookies directory."""

    _VALID_NAMES: frozenset[str] = frozenset(
        {
            "instagram.com_cookies.txt",
            "tiktok.com_cookies.txt",
            "facebook.com_cookies.txt",
            "x.com_cookies.txt",
        }
    )

    def __init__(self, cfg: "Config") -> None:
        self._dir = cfg.cookies_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def is_valid_name(self, name: str) -> bool:
        return name in self._VALID_NAMES

    def save(self, name: str, data: bytes) -> Path:
        """Write cookie bytes atomically. Returns destination path."""
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

    def list_all(self) -> list[str]:
        return sorted(
            f.name for f in self._dir.iterdir()
            if f.is_file() and f.suffix == ".txt"
        )
