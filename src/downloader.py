"""
downloader.py — Production-grade media downloader.
Instagram → instaloader (mobile API, no cookie needed)
TikTok / Facebook / X → gallery-dl (cookie-based)
"""

from __future__ import annotations

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
from urllib.parse import urlparse

import instaloader

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────

SUPPORTED_PLATFORMS = ("instagram", "tiktok", "facebook", "x")

_INSTAGRAM_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/"
    r"(?:p|reel|reels|tv|stories)/([A-Za-z0-9_\-]+)"
)
_TIKTOK_RE = re.compile(r"https?://(?:www\.|vm\.|vt\.)?tiktok\.com/")
_FACEBOOK_RE = re.compile(r"https?://(?:www\.|m\.)?facebook\.com/")
_X_RE = re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/")

_MAX_FILE_SIZE_MB = int(os.environ.get("MAX_FILE_SIZE_MB", "50"))
_GALLERY_DL_BIN = shutil.which("gallery-dl") or "gallery-dl"


# ──────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────

@dataclass
class DownloadResult:
    success: bool
    files: List[Path] = field(default_factory=list)
    caption: Optional[str] = None
    error: Optional[str] = None
    platform: Optional[str] = None


# ──────────────────────────────────────────────
# Platform detection
# ──────────────────────────────────────────────

def detect_platform(url: str) -> Optional[str]:
    if _INSTAGRAM_RE.search(url):
        return "instagram"
    if _TIKTOK_RE.search(url):
        return "tiktok"
    if _FACEBOOK_RE.search(url):
        return "facebook"
    if _X_RE.search(url):
        return "x"
    return None


# ──────────────────────────────────────────────
# Instagram — instaloader (no cookie required)
# ──────────────────────────────────────────────

class InstaloaderDownloader:
    """
    Downloads Instagram posts/reels/stories via instaloader.
    Uses the public mobile API — no cookie, no 429 from datacenter IPs.
    Optionally logs in with INSTAGRAM_USERNAME + INSTAGRAM_PASSWORD env vars
    to access private content or avoid occasional rate limits.
    """

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
        self._login()

    def _login(self) -> None:
        username = os.environ.get("INSTAGRAM_USERNAME", "").strip()
        password = os.environ.get("INSTAGRAM_PASSWORD", "").strip()
        if username and password:
            try:
                self._loader.login(username, password)
                logger.info("instaloader: logged in as %s", username)
            except instaloader.exceptions.BadCredentialsException:
                logger.warning("instaloader: bad credentials — running anonymously")
            except Exception as exc:  # noqa: BLE001
                logger.warning("instaloader: login failed (%s) — running anonymously", exc)
        else:
            logger.info("instaloader: running anonymously (no INSTAGRAM_USERNAME set)")

    # ------------------------------------------------------------------
    def download(self, url: str, dest_dir: Path) -> DownloadResult:
        dest_dir.mkdir(parents=True, exist_ok=True)
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return DownloadResult(
                success=False,
                error="Could not extract Instagram shortcode from URL.",
                platform="instagram",
            )

        try:
            post = instaloader.Post.from_shortcode(self._loader.context, shortcode)
        except instaloader.exceptions.InstaloaderException as exc:
            return DownloadResult(
                success=False,
                error=f"Instagram fetch failed: {exc}",
                platform="instagram",
            )
        except Exception as exc:  # noqa: BLE001
            return DownloadResult(
                success=False,
                error=f"Unexpected error fetching post: {exc}",
                platform="instagram",
            )

        caption = post.caption or ""

        try:
            # instaloader downloads into CWD; temporarily change to dest_dir
            original_cwd = Path.cwd()
            os.chdir(dest_dir)
            self._loader.download_post(post, target=dest_dir / shortcode)
            os.chdir(original_cwd)
        except instaloader.exceptions.InstaloaderException as exc:
            os.chdir(original_cwd)
            return DownloadResult(
                success=False,
                error=f"Instagram download failed: {exc}",
                platform="instagram",
            )
        except Exception as exc:  # noqa: BLE001
            try:
                os.chdir(original_cwd)
            except Exception:
                pass
            return DownloadResult(
                success=False,
                error=f"Unexpected download error: {exc}",
                platform="instagram",
            )

        files = self._collect_media_files(dest_dir)
        if not files:
            return DownloadResult(
                success=False,
                error="Download succeeded but no media files found.",
                platform="instagram",
            )

        files = self._filter_by_size(files)
        return DownloadResult(
            success=True,
            files=files,
            caption=caption[:1024] if caption else None,
            platform="instagram",
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _extract_shortcode(url: str) -> Optional[str]:
        match = _INSTAGRAM_RE.search(url)
        return match.group(1) if match else None

    @staticmethod
    def _collect_media_files(directory: Path) -> List[Path]:
        extensions = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v"}
        files: List[Path] = []
        for path in sorted(directory.rglob("*")):
            if path.is_file() and path.suffix.lower() in extensions:
                files.append(path)
        return files

    @staticmethod
    def _filter_by_size(files: List[Path]) -> List[Path]:
        limit = _MAX_FILE_SIZE_MB * 1024 * 1024
        filtered = [f for f in files if f.stat().st_size <= limit]
        skipped = len(files) - len(filtered)
        if skipped:
            logger.warning("Skipped %d file(s) exceeding %d MB limit", skipped, _MAX_FILE_SIZE_MB)
        return filtered


# ──────────────────────────────────────────────
# gallery-dl — TikTok / Facebook / X
# ──────────────────────────────────────────────

class GalleryDlDownloader:
    """
    Downloads TikTok, Facebook, and X media via gallery-dl.
    Reads cookies from COOKIE_<PLATFORM> environment variables.
    """

    def __init__(self) -> None:
        self._cookie_files: dict[str, Path] = {}
        self._tmp_dir = Path(tempfile.mkdtemp(prefix="gdl_cookies_"))
        self._load_cookies()

    def _load_cookies(self) -> None:
        for platform in ("tiktok", "facebook", "x"):
            env_key = f"COOKIE_{platform.upper()}"
            cookie_data = os.environ.get(env_key, "").strip()
            if not cookie_data:
                logger.info("Cookie [%s]: ❌ missing", platform)
                continue

            cookie_path = self._tmp_dir / f"{platform}.txt"
            cookie_path.write_text(cookie_data, encoding="utf-8")
            size = len(cookie_data.encode())
            self._cookie_files[platform] = cookie_path

            if size < 2000:
                logger.warning(
                    "Cookie [%s]: ✅ %d bytes ⚠️  (too small — may cause 429)",
                    platform, size,
                )
            else:
                logger.info("Cookie [%s]: ✅ %d bytes", platform, size)

    # ------------------------------------------------------------------
    def download(self, url: str, platform: str, dest_dir: Path) -> DownloadResult:
        dest_dir.mkdir(parents=True, exist_ok=True)
        config_path = self._write_config(platform, dest_dir)

        cmd = [
            _GALLERY_DL_BIN,
            "--config", str(config_path),
            "--no-mtime",
            url,
        ]

        logger.info("gallery-dl cmd: %s %s", " ".join(cmd[:-1]), url)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except subprocess.TimeoutExpired:
            return DownloadResult(
                success=False,
                error="gallery-dl timed out after 120 seconds.",
                platform=platform,
            )
        except FileNotFoundError:
            return DownloadResult(
                success=False,
                error="gallery-dl binary not found. Ensure it is installed.",
                platform=platform,
            )

        if result.returncode not in (0, 1):
            logger.error("gallery-dl stderr: %s", result.stderr[:500])
            return DownloadResult(
                success=False,
                error=f"gallery-dl exited {result.returncode}: {result.stderr[:300]}",
                platform=platform,
            )

        files = self._collect_media_files(dest_dir)
        if not files:
            hint = " (cookie may be expired or missing)" if platform != "tiktok" else ""
            return DownloadResult(
                success=False,
                error=f"No media files downloaded{hint}.",
                platform=platform,
            )

        files = self._filter_by_size(files)
        return DownloadResult(
            success=True,
            files=files,
            platform=platform,
        )

    # ------------------------------------------------------------------
    def _write_config(self, platform: str, dest_dir: Path) -> Path:
        cookie_path = self._cookie_files.get(platform)

        extractor_cfg: dict = {
            "directory": [str(dest_dir)],
            "filename": "{id}.{extension}",
            "sleep-request": 3,
            "retries": 3,
            "timeout": 30,
        }

        if cookie_path:
            extractor_cfg["cookies"] = str(cookie_path)

        proxy = os.environ.get("PROXY", os.environ.get("HTTP_PROXY", "")).strip()
        if proxy:
            extractor_cfg["proxy"] = proxy

        config = {
            "extractor": {
                platform: extractor_cfg,
            },
            "downloader": {
                "part": False,
                "retries": 3,
                "timeout": 60,
            },
        }

        cfg_path = self._tmp_dir / f"gdl_cfg_{platform}_{int(time.time())}.json"
        cfg_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return cfg_path

    @staticmethod
    def _collect_media_files(directory: Path) -> List[Path]:
        extensions = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v", ".gif"}
        return sorted(
            p for p in directory.rglob("*")
            if p.is_file() and p.suffix.lower() in extensions
        )

    @staticmethod
    def _filter_by_size(files: List[Path]) -> List[Path]:
        limit = _MAX_FILE_SIZE_MB * 1024 * 1024
        filtered = [f for f in files if f.stat().st_size <= limit]
        skipped = len(files) - len(filtered)
        if skipped:
            logger.warning("Skipped %d file(s) exceeding %d MB limit", skipped, _MAX_FILE_SIZE_MB)
        return filtered


# ──────────────────────────────────────────────
# Unified public API
# ──────────────────────────────────────────────

class Downloader:
    """
    Single entry point for all platforms.

    Usage:
        dl = Downloader()
        result = dl.download("https://www.instagram.com/p/XYZ/", Path("/tmp/dl"))
    """

    def __init__(self) -> None:
        self._insta = InstaloaderDownloader()
        self._gdl = GalleryDlDownloader()

    # ------------------------------------------------------------------
    def download(self, url: str, dest_dir: Optional[Path] = None) -> DownloadResult:
        platform = detect_platform(url)
        if platform is None:
            return DownloadResult(
                success=False,
                error="Unsupported URL. Supported: Instagram, TikTok, Facebook, X/Twitter.",
            )

        work_dir = dest_dir or Path(tempfile.mkdtemp(prefix="sarada_dl_"))
        work_dir.mkdir(parents=True, exist_ok=True)

        if platform == "instagram":
            return self._insta.download(url, work_dir)

        return self._gdl.download(url, platform, work_dir)

    # ------------------------------------------------------------------
    @staticmethod
    def is_supported(url: str) -> bool:
        return detect_platform(url) is not None

    @staticmethod
    def supported_platforms() -> tuple[str, ...]:
        return SUPPORTED_PLATFORMS
