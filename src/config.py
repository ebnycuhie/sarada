"""
config.py — Central configuration loaded from environment variables.

Supports setting cookies directly via Railway environment variables:
  COOKIE_INSTAGRAM  → base64-encoded instagram.com_cookies.txt content
  COOKIE_TIKTOK     → base64-encoded tiktok.com_cookies.txt content
  COOKIE_FACEBOOK   → base64-encoded facebook.com_cookies.txt content
  COOKIE_X          → base64-encoded x.com_cookies.txt content

These are decoded and written to /data/cookies/ on startup if the cookie
file does not already exist. This allows cookie injection without file uploads.
"""

from __future__ import annotations

import base64
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


def _require(key: str) -> str:
    value = os.environ.get(key, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable '{key}' is not set.")
    return value


def _int_env(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except ValueError:
        return default


def _path_env(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default))


@dataclass(frozen=True)
class PlatformConfig:
    name:        str
    label:       str
    url_prefix:  str
    cookie_file: str
    sleep_sec:   str
    folder:      str


@dataclass(frozen=True)
class Config:
    # ── Telegram ──────────────────────────────────────────────────────────────
    bot_token:     str
    owner_id:      int

    # ── Paths ─────────────────────────────────────────────────────────────────
    base_dir:      Path
    cookies_dir:   Path
    profiles_file: Path
    log_file:      Path

    # ── Download limits ───────────────────────────────────────────────────────
    max_send_files:   int
    max_file_size_mb: int
    max_concurrent:   int

    # ── Platform registry ─────────────────────────────────────────────────────
    platforms: dict[str, PlatformConfig] = field(default_factory=dict)

    # ── gallery-dl filters ────────────────────────────────────────────────────
    photo_exts: frozenset[str] = frozenset(
        {"jpg", "jpeg", "png", "gif", "webp", "bmp"}
    )
    video_exts: frozenset[str] = frozenset(
        {"mp4", "webm", "mkv", "mov", "avi", "m4v"}
    )

    @property
    def photo_filter(self) -> str:
        quoted = ", ".join(f"'{e}'" for e in sorted(self.photo_exts))
        return f"extension in ({quoted})"

    @property
    def video_filter(self) -> str:
        quoted = ", ".join(f"'{e}'" for e in sorted(self.video_exts))
        return f"extension in ({quoted})"


# ── Cookie env-var injection ───────────────────────────────────────────────────

_COOKIE_ENV_MAP: dict[str, str] = {
    "COOKIE_INSTAGRAM": "instagram.com_cookies.txt",
    "COOKIE_TIKTOK":    "tiktok.com_cookies.txt",
    "COOKIE_FACEBOOK":  "facebook.com_cookies.txt",
    "COOKIE_X":         "x.com_cookies.txt",
}


def _inject_env_cookies(cookies_dir: Path) -> None:
    """
    If COOKIE_* env vars are set, decode them (base64 or plain text) and
    write them to the cookies directory. Existing files are NOT overwritten
    so that manually uploaded cookies take precedence.
    """
    cookies_dir.mkdir(parents=True, exist_ok=True)
    for env_key, filename in _COOKIE_ENV_MAP.items():
        value = os.environ.get(env_key, "").strip()
        if not value:
            continue
        dest = cookies_dir / filename
        if dest.exists():
            logger.debug("Cookie env-var %s ignored — %s already exists.", env_key, filename)
            continue
        try:
            # Try base64 decode first; fall back to raw text
            try:
                data = base64.b64decode(value)
            except Exception:
                data = value.encode()
            dest.write_bytes(data)
            logger.info(
                "Cookie injected from env %s → %s (%d bytes)",
                env_key, filename, len(data),
            )
        except OSError as exc:
            logger.error("Failed to write cookie from %s: %s", env_key, exc)


# ── load ───────────────────────────────────────────────────────────────────────

def load() -> Config:
    """Build and return the singleton Config from the environment."""
    base_dir    = _path_env("DOWNLOAD_DIR",   "/data/downloads")
    cookies_dir = _path_env("COOKIES_DIR",    "/data/cookies")
    data_dir    = _path_env("DATA_DIR",       "/data")
    log_dir     = _path_env("LOG_DIR",        "/data/logs")

    platforms: dict[str, PlatformConfig] = {
        "instagram": PlatformConfig(
            name="instagram",
            label="INSTAGRAM",
            url_prefix="https://www.instagram.com/",
            cookie_file="instagram.com_cookies.txt",
            sleep_sec="5",
            folder="Instagram",
        ),
        "tiktok": PlatformConfig(
            name="tiktok",
            label="TIKTOK",
            url_prefix="https://www.tiktok.com/@",
            cookie_file="tiktok.com_cookies.txt",
            sleep_sec="3",
            folder="TikTok",
        ),
        "facebook": PlatformConfig(
            name="facebook",
            label="FACEBOOK",
            url_prefix="https://www.facebook.com/",
            cookie_file="facebook.com_cookies.txt",
            sleep_sec="5",
            folder="Facebook",
        ),
        "x": PlatformConfig(
            name="x",
            label="X / TWITTER",
            url_prefix="https://x.com/",
            cookie_file="x.com_cookies.txt",
            sleep_sec="5",
            folder="X",
        ),
    }

    cfg = Config(
        bot_token=_require("BOT_TOKEN"),
        owner_id=_int_env("OWNER_ID", 0),
        base_dir=base_dir,
        cookies_dir=cookies_dir,
        profiles_file=data_dir / "profiles.json",
        log_file=log_dir / "bot.log",
        max_send_files=_int_env("MAX_SEND_FILES", 20),
        max_file_size_mb=min(_int_env("MAX_FILE_SIZE_MB", 50), 50),
        max_concurrent=_int_env("MAX_CONCURRENT", 1),
        platforms=platforms,
    )

    # Inject cookies from env vars (Railway variables alternative to file upload)
    _inject_env_cookies(cookies_dir)

    return cfg
