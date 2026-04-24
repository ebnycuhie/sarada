"""
config.py — Central configuration loaded from environment variables.

Cookie injection order (highest priority wins):
  1. Manually uploaded file in cookies_dir  (via Telegram document upload)
  2. COOKIE_* environment variable          (Railway variable — plain text OR base64)

On every startup the env-var cookies are ALWAYS written so Railway variable
updates take effect without a re-upload.  A manually uploaded file uploaded
*after* startup overwrites the env-var version in place.

Supported env vars:
  COOKIE_INSTAGRAM  → instagram.com_cookies.txt
  COOKIE_TIKTOK     → tiktok.com_cookies.txt
  COOKIE_FACEBOOK   → facebook.com_cookies.txt
  COOKIE_X          → x.com_cookies.txt
"""

from __future__ import annotations

import base64
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Minimum cookie size that gallery-dl needs to avoid 429 ────────────────────
MIN_COOKIE_BYTES: int = 2_000


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


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PlatformConfig:
    name: str
    label: str
    url_prefix: str
    cookie_file: str
    sleep_sec: str
    folder: str


@dataclass(frozen=True)
class Config:
    # ── Telegram ──────────────────────────────────────────────────────────────
    bot_token: str
    owner_id: int

    # ── Paths ─────────────────────────────────────────────────────────────────
    base_dir: Path
    cookies_dir: Path
    profiles_file: Path
    log_file: Path

    # ── Download limits ───────────────────────────────────────────────────────
    max_send_files: int
    max_file_size_mb: int
    max_concurrent: int

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


def _decode_cookie_value(value: str) -> bytes:
    """
    Accept either:
      • Plain Netscape cookie text (starts with '#' or '.domain')
      • Base64-encoded cookie text

    Returns raw bytes ready to write to disk.
    """
    stripped = value.strip()

    # Fast path: plain-text Netscape cookie file
    if stripped.startswith("#") or stripped.startswith(".") or "\t" in stripped:
        return stripped.encode("utf-8")

    # Try base64 decode
    try:
        decoded = base64.b64decode(stripped)
        # Validate it looks like a cookie file after decoding
        text = decoded.decode("utf-8", errors="replace")
        if "#" in text or "\t" in text or ".instagram" in text:
            return decoded
    except Exception:
        pass

    # Fallback: treat as plain text regardless
    return stripped.encode("utf-8")


def _deduplicate_cookie_lines(raw: bytes) -> bytes:
    """
    Remove duplicate cookie entries from a Netscape cookie file.
    Keeps the last occurrence of each (domain, name) pair so that
    the freshest value wins.  Header comment lines are preserved.
    """
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        return raw

    header_lines: list[str] = []
    cookie_map: dict[tuple[str, str], str] = {}   # (domain, name) → full line

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            header_lines.append(line)
            continue
        parts = stripped.split("\t")
        if len(parts) >= 6:
            domain = parts[0]
            name = parts[5]
            cookie_map[(domain, name)] = line
        else:
            # Malformed line — keep it verbatim
            header_lines.append(line)

    result_lines = header_lines + list(cookie_map.values())
    return "\n".join(result_lines).encode("utf-8")


def inject_env_cookies(cookies_dir: Path) -> None:
    """
    Write COOKIE_* env vars to disk on every startup.
    Env vars are ALWAYS written (not skipped if file exists) so that
    updating the Railway variable takes effect without a re-upload.

    A cookie uploaded manually via Telegram will overwrite the file on
    disk at upload time (handled by CookieStore.save), so manual uploads
    always remain authoritative for the current process lifetime.
    """
    cookies_dir.mkdir(parents=True, exist_ok=True)

    for env_key, filename in _COOKIE_ENV_MAP.items():
        value = os.environ.get(env_key, "").strip()
        if not value:
            continue

        dest = cookies_dir / filename

        try:
            raw = _decode_cookie_value(value)
            raw = _deduplicate_cookie_lines(raw)

            if len(raw) < MIN_COOKIE_BYTES:
                logger.warning(
                    "Cookie from %s is only %d bytes — likely missing session "
                    "cookies; gallery-dl may receive 429 errors.",
                    env_key, len(raw),
                )

            dest.write_bytes(raw)
            logger.info(
                "Cookie written from env %s → %s (%d bytes)",
                env_key, filename, len(raw),
            )
        except OSError as exc:
            logger.error("Failed to write cookie from %s: %s", env_key, exc)


# ── load ───────────────────────────────────────────────────────────────────────

def load() -> Config:
    """Build and return the singleton Config from the environment."""
    base_dir    = _path_env("DOWNLOAD_DIR", "/data/downloads")
    cookies_dir = _path_env("COOKIES_DIR",  "/data/cookies")
    data_dir    = _path_env("DATA_DIR",     "/data")
    log_dir     = _path_env("LOG_DIR",      "/data/logs")
    owner_id    = _int_env("OWNER_ID", 0)

    if owner_id <= 0:
        raise RuntimeError("Required environment variable 'OWNER_ID' must be a non-zero integer.")

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
        owner_id=owner_id,
        base_dir=base_dir,
        cookies_dir=cookies_dir,
        profiles_file=data_dir / "profiles.json",
        log_file=log_dir / "bot.log",
        max_send_files=_int_env("MAX_SEND_FILES", 20),
        max_file_size_mb=min(_int_env("MAX_FILE_SIZE_MB", 50), 50),
        max_concurrent=_int_env("MAX_CONCURRENT", 1),
        platforms=platforms,
    )

    # Always inject / refresh cookies from env vars on startup
    inject_env_cookies(cookies_dir)

    return cfg
