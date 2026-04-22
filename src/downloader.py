"""
downloader.py — Production-grade media downloader for Sarada bot.

Exports (required by handlers.py):
    - Downloader     : main download class
    - ErrorKind      : enum of failure categories
    - MediaMode      : enum for photo / video / both

Instagram  → instaloader  (mobile API — no cookies, no 429 on Railway)
TikTok     → gallery-dl   (cookie optional)
Facebook   → gallery-dl   (cookie recommended)
X/Twitter  → gallery-dl   (cookie recommended)
"""

from __future__ import annotations

import enum
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import instaloader

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Public enums  (imported by handlers.py)
# ─────────────────────────────────────────────────────────────────────────────

class MediaMode(enum.Enum):
    """Which media types to download."""
    PHOTOS = "photos"
    VIDEOS = "videos"
    BOTH   = "both"


class ErrorKind(enum.Enum):
    """Categorised failure reasons for the bot to give useful replies."""
    UNSUPPORTED_URL    = "unsupported_url"
    PRIVATE_CONTENT    = "private_content"
    NOT_FOUND          = "not_found"
    RATE_LIMITED       = "rate_limited"
    NO_MEDIA           = "no_media"
    FILE_TOO_LARGE     = "file_too_large"
    TIMEOUT            = "timeout"
    COOKIE_MISSING     = "cookie_missing"
    COOKIE_EXPIRED     = "cookie_expired"
    UNKNOWN            = "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Internal constants
# ─────────────────────────────────────────────────────────────────────────────

_MAX_FILE_MB   = int(os.environ.get("MAX_FILE_SIZE_MB", "50"))
_GALLERY_DL    = shutil.which("gallery-dl") or "gallery-dl"

_RE_INSTAGRAM  = re.compile(
    r"https?://(?:www\.)?instagram\.com/"
    r"(?:p|reel|reels|tv|stories)/([A-Za-z0-9_\-]+)"
)
_RE_TIKTOK     = re.compile(r"https?://(?:www\.|vm\.|vt\.)?tiktok\.com/")
_RE_FACEBOOK   = re.compile(r"https?://(?:www\.|m\.)?facebook\.com/")
_RE_X          = re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/")

_MEDIA_EXTS    = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v"}
_PHOTO_EXTS    = {".jpg", ".jpeg", ".png", ".webp"}
_VIDEO_EXTS    = {".mp4", ".mov", ".m4v"}


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DownloadResult:
    success:    bool
    files:      List[Path]        = field(default_factory=list)
    caption:    Optional[str]     = None
    error:      Optional[str]     = None
    error_kind: Optional[ErrorKind] = None
    platform:   Optional[str]    = None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _detect_platform(url: str) -> Optional[str]:
    if _RE_INSTAGRAM.search(url):  return "instagram"
    if _RE_TIKTOK.search(url):     return "tiktok"
    if _RE_FACEBOOK.search(url):   return "facebook"
    if _RE_X.search(url):          return "x"
    return None


def _filter_by_mode(files: List[Path], mode: MediaMode) -> List[Path]:
    if mode == MediaMode.PHOTOS:
        return [f for f in files if f.suffix.lower() in _PHOTO_EXTS]
    if mode == MediaMode.VIDEOS:
        return [f for f in files if f.suffix.lower() in _VIDEO_EXTS]
    return files  # BOTH


def _filter_by_size(files: List[Path]) -> List[Path]:
    limit = _MAX_FILE_MB * 1024 * 1024
    kept, dropped = [], 0
    for f in files:
        if f.stat().st_size <= limit:
            kept.append(f)
        else:
            dropped += 1
    if dropped:
        logger.warning("Dropped %d file(s) over %d MB", dropped, _MAX_FILE_MB)
    return kept


def _collect(directory: Path, exts: set) -> List[Path]:
    return sorted(p for p in directory.rglob("*")
                  if p.is_file() and p.suffix.lower() in exts)


# ─────────────────────────────────────────────────────────────────────────────
# Instagram via instaloader
# ─────────────────────────────────────────────────────────────────────────────

class _InstaLoader:
    def __init__(self) -> None:
        self._loader = instaloader.Instaloader(
            download_pictures=True,
            download_videos=True,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            post_metadata_txt_pattern="",
            max_connection_attempts=3,
            request_timeout=30,
            quiet=True,
        )
        self._try_login()

    def _try_login(self) -> None:
        username = os.environ.get("INSTAGRAM_USERNAME", "").strip()
        password = os.environ.get("INSTAGRAM_PASSWORD", "").strip()
        if username and password:
            try:
                self._loader.login(username, password)
                logger.info("instaloader: logged in as %s", username)
            except instaloader.exceptions.BadCredentialsException:
                logger.warning("instaloader: bad credentials — anonymous mode")
            except Exception as exc:
                logger.warning("instaloader: login failed (%s) — anonymous mode", exc)
        else:
            logger.info("instaloader: anonymous mode (set INSTAGRAM_USERNAME to log in)")

    def download(self, url: str, dest: Path, mode: MediaMode) -> DownloadResult:
        dest.mkdir(parents=True, exist_ok=True)
        m = _RE_INSTAGRAM.search(url)
        if not m:
            return DownloadResult(
                success=False,
                error="Could not parse Instagram shortcode.",
                error_kind=ErrorKind.UNSUPPORTED_URL,
                platform="instagram",
            )
        shortcode = m.group(1)

        try:
            post = instaloader.Post.from_shortcode(self._loader.context, shortcode)
        except instaloader.exceptions.LoginRequiredException:
            return DownloadResult(
                success=False,
                error="This Instagram post is private.",
                error_kind=ErrorKind.PRIVATE_CONTENT,
                platform="instagram",
            )
        except instaloader.exceptions.QueryReturnedNotFoundException:
            return DownloadResult(
                success=False,
                error="Instagram post not found.",
                error_kind=ErrorKind.NOT_FOUND,
                platform="instagram",
            )
        except Exception as exc:
            return DownloadResult(
                success=False,
                error=f"Failed to fetch Instagram post: {exc}",
                error_kind=ErrorKind.UNKNOWN,
                platform="instagram",
            )

        caption = (post.caption or "")[:1024]
        original_cwd = Path.cwd()
        try:
            os.chdir(dest)
            self._loader.download_post(post, target=dest / shortcode)
        except instaloader.exceptions.InstaloaderException as exc:
            return DownloadResult(
                success=False,
                error=f"Instagram download error: {exc}",
                error_kind=ErrorKind.UNKNOWN,
                platform="instagram",
            )
        except Exception as exc:
            return DownloadResult(
                success=False,
                error=f"Unexpected error: {exc}",
                error_kind=ErrorKind.UNKNOWN,
                platform="instagram",
            )
        finally:
            os.chdir(original_cwd)

        files = _filter_by_mode(_collect(dest, _MEDIA_EXTS), mode)
        files = _filter_by_size(files)

        if not files:
            return DownloadResult(
                success=False,
                error="No media files found after download.",
                error_kind=ErrorKind.NO_MEDIA,
                platform="instagram",
            )

        return DownloadResult(
            success=True,
            files=files,
            caption=caption or None,
            platform="instagram",
        )


# ─────────────────────────────────────────────────────────────────────────────
# TikTok / Facebook / X via gallery-dl
# ─────────────────────────────────────────────────────────────────────────────

class _GalleryDl:
    def __init__(self) -> None:
        self._tmp   = Path(tempfile.mkdtemp(prefix="sarada_gdl_"))
        self._cookies: dict[str, Path] = {}
        self._load_cookies()

    def _load_cookies(self) -> None:
        for platform in ("tiktok", "facebook", "x"):
            raw = os.environ.get(f"COOKIE_{platform.upper()}", "").strip()
            if not raw:
                logger.info("Cookie [%s]: ❌ missing", platform)
                continue
            path = self._tmp / f"{platform}.txt"
            path.write_text(raw, encoding="utf-8")
            size = len(raw.encode())
            self._cookies[platform] = path
            if size < 2000:
                logger.warning("Cookie [%s]: ✅ %d bytes ⚠️  (may cause 429)", platform, size)
            else:
                logger.info("Cookie [%s]: ✅ %d bytes", platform, size)

    def download(self, url: str, platform: str, dest: Path, mode: MediaMode) -> DownloadResult:
        dest.mkdir(parents=True, exist_ok=True)
        cfg_path = self._write_config(platform, dest)

        cmd = [_GALLERY_DL, "--config", str(cfg_path), "--no-mtime", url]
        logger.info("gallery-dl: %s", url)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            return DownloadResult(
                success=False,
                error="Download timed out after 120 s.",
                error_kind=ErrorKind.TIMEOUT,
                platform=platform,
            )
        except FileNotFoundError:
            return DownloadResult(
                success=False,
                error="gallery-dl not installed.",
                error_kind=ErrorKind.UNKNOWN,
                platform=platform,
            )

        stderr = result.stderr or ""
        if "HTTP Error 429" in stderr or "Too Many Requests" in stderr:
            return DownloadResult(
                success=False,
                error="Rate limited (429). Try again later.",
                error_kind=ErrorKind.RATE_LIMITED,
                platform=platform,
            )
        if "HTTP Error 404" in stderr or "does not exist" in stderr.lower():
            return DownloadResult(
                success=False,
                error="Content not found.",
                error_kind=ErrorKind.NOT_FOUND,
                platform=platform,
            )
        if "login" in stderr.lower() or "private" in stderr.lower():
            return DownloadResult(
                success=False,
                error="Content is private or requires login.",
                error_kind=ErrorKind.PRIVATE_CONTENT,
                platform=platform,
            )

        files = _filter_by_mode(_collect(dest, _MEDIA_EXTS), mode)
        files = _filter_by_size(files)

        if not files:
            kind = ErrorKind.COOKIE_MISSING if platform not in self._cookies else ErrorKind.NO_MEDIA
            return DownloadResult(
                success=False,
                error=f"No media downloaded. {'Cookie missing.' if kind == ErrorKind.COOKIE_MISSING else ''}",
                error_kind=kind,
                platform=platform,
            )

        return DownloadResult(success=True, files=files, platform=platform)

    def _write_config(self, platform: str, dest: Path) -> Path:
        ext_cfg: dict = {
            "directory": [str(dest)],
            "filename":  "{id}.{extension}",
            "sleep-request": 3,
            "retries":   3,
            "timeout":   30,
        }
        if platform in self._cookies:
            ext_cfg["cookies"] = str(self._cookies[platform])

        proxy = os.environ.get("PROXY", os.environ.get("HTTP_PROXY", "")).strip()
        if proxy:
            ext_cfg["proxy"] = proxy

        cfg = {
            "extractor": {platform: ext_cfg},
            "downloader": {"part": False, "retries": 3, "timeout": 60},
        }
        path = self._tmp / f"cfg_{platform}_{int(time.time())}.json"
        path.write_text(json.dumps(cfg), encoding="utf-8")
        return path


# ─────────────────────────────────────────────────────────────────────────────
# Public façade  (imported by handlers.py as `Downloader`)
# ─────────────────────────────────────────────────────────────────────────────

class Downloader:
    """
    Single entry point for all platforms.

        dl = Downloader()
        result = dl.download(url, mode=MediaMode.BOTH)
    """

    def __init__(self) -> None:
        self._insta = _InstaLoader()
        self._gdl   = _GalleryDl()

    # ------------------------------------------------------------------
    def download(
        self,
        url: str,
        mode: MediaMode = MediaMode.BOTH,
        dest_dir: Optional[Path] = None,
    ) -> DownloadResult:
        platform = _detect_platform(url)
        if platform is None:
            return DownloadResult(
                success=False,
                error="Unsupported URL. Supported: Instagram, TikTok, Facebook, X/Twitter.",
                error_kind=ErrorKind.UNSUPPORTED_URL,
            )

        work_dir = dest_dir or Path(tempfile.mkdtemp(prefix="sarada_dl_"))
        work_dir.mkdir(parents=True, exist_ok=True)

        if platform == "instagram":
            return self._insta.download(url, work_dir, mode)
        return self._gdl.download(url, platform, work_dir, mode)

    # ------------------------------------------------------------------
    @staticmethod
    def is_supported(url: str) -> bool:
        return _detect_platform(url) is not None

    @staticmethod
    def supported_platforms() -> tuple:
        return ("instagram", "tiktok", "facebook", "x")
