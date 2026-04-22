"""
downloader.py — Production-grade media downloader for the Sarada Telegram bot.

Instagram  → instaloader  (mobile API, no 429 from datacenter IPs)
TikTok     → gallery-dl
Facebook   → gallery-dl
X/Twitter  → gallery-dl
"""

from __future__ import annotations

import asyncio
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
from urllib.parse import urlparse

import instaloader

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public enumerations (imported by handlers.py)
# ---------------------------------------------------------------------------

class ErrorKind(enum.Enum):
    UNSUPPORTED_URL   = "unsupported_url"
    PRIVATE_CONTENT   = "private_content"
    NOT_FOUND         = "not_found"
    RATE_LIMITED      = "rate_limited"
    NO_MEDIA          = "no_media"
    FILE_TOO_LARGE    = "file_too_large"
    TIMEOUT           = "timeout"
    COOKIE_MISSING    = "cookie_missing"
    COOKIE_EXPIRED    = "cookie_expired"
    UNKNOWN           = "unknown"


class MediaMode(enum.Enum):
    PHOTOS = "photos"
    VIDEOS = "videos"
    BOTH   = "both"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class DownloadResult:
    success:    bool
    files:      List[Path]              = field(default_factory=list)
    error_kind: Optional[ErrorKind]     = None
    error_msg:  str                     = ""

    # Convenience helpers
    @staticmethod
    def ok(files: List[Path]) -> "DownloadResult":
        return DownloadResult(success=True, files=files)

    @staticmethod
    def fail(kind: ErrorKind, msg: str = "") -> "DownloadResult":
        return DownloadResult(success=False, error_kind=kind, error_msg=msg)


# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

_INSTAGRAM_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/"
    r"(?:p|reel|tv|stories|[a-zA-Z0-9_.]+)/",
    re.IGNORECASE,
)
_TIKTOK_RE   = re.compile(r"https?://(?:www\.|vm\.|vt\.)?tiktok\.com/", re.IGNORECASE)
_FACEBOOK_RE = re.compile(r"https?://(?:www\.|m\.)?(?:facebook|fb)\.com/", re.IGNORECASE)
_X_RE        = re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/", re.IGNORECASE)


def _detect_platform(url: str) -> Optional[str]:
    if _INSTAGRAM_RE.search(url):
        return "instagram"
    if _TIKTOK_RE.search(url):
        return "tiktok"
    if _FACEBOOK_RE.search(url):
        return "facebook"
    if _X_RE.search(url):
        return "x"
    return None


# ---------------------------------------------------------------------------
# Config wrapper (mirrors what bot.py / handlers.py passes in)
# ---------------------------------------------------------------------------

class _Config:
    """Thin wrapper around the cfg dict/object the bot passes to Downloader."""

    def __init__(self, cfg) -> None:
        # Support both dict and object-style configs
        def _get(key: str, default=None):
            if isinstance(cfg, dict):
                return cfg.get(key, default)
            return getattr(cfg, key, default)

        self.base_dir: Path        = Path(_get("base_dir", "/data/downloads"))
        self.max_filesize_mb: int  = int(_get("max_filesize_mb", 50))
        self.timeout_sec: int      = int(_get("timeout_sec", 120))
        self.proxy: Optional[str]  = _get("proxy") or os.environ.get("INSTAGRAM_PROXY")
        self.ig_username: Optional[str] = (
            _get("instagram_username")
            or os.environ.get("INSTAGRAM_USERNAME")
        )
        self.ig_password: Optional[str] = (
            _get("instagram_password")
            or os.environ.get("INSTAGRAM_PASSWORD")
        )

        # gallery-dl cookie files (written by bot.py / config.py)
        self.gdl_config_path: Optional[Path] = (
            Path(p) if (p := _get("gdl_config_path")) else None
        )
        # Also accept the already-written path stored as string env
        self._cookie_dir: Optional[Path] = (
            Path(_get("cookie_dir")) if _get("cookie_dir") else None
        )

        self.base_dir.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Main Downloader class
# ---------------------------------------------------------------------------

class Downloader:
    """
    Unified async downloader.

    Constructed with the same `cfg` object that bot.py passes to BotHandlers:
        self._dl = Downloader(cfg)
    """

    def __init__(self, cfg) -> None:  # ← MUST accept cfg as positional arg
        self._cfg = _Config(cfg)
        self._il: Optional[instaloader.Instaloader] = None
        self._il_logged_in = False
        log.info("downloader: Downloader initialised (Instagram=instaloader, others=gallery-dl)")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def download(
        self,
        url: str,
        *,
        mode: MediaMode = MediaMode.BOTH,
        dest_dir: Optional[Path] = None,
    ) -> DownloadResult:
        """
        Download media from *url*.

        Returns a DownloadResult with .files populated on success,
        or .error_kind / .error_msg on failure.
        """
        platform = _detect_platform(url)
        if platform is None:
            return DownloadResult.fail(ErrorKind.UNSUPPORTED_URL, f"Unsupported URL: {url}")

        work_dir = dest_dir or self._cfg.base_dir / _safe_name(url)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            if platform == "instagram":
                return await asyncio.get_event_loop().run_in_executor(
                    None, self._download_instagram, url, work_dir, mode
                )
            else:
                return await asyncio.get_event_loop().run_in_executor(
                    None, self._download_gallery_dl, url, work_dir, platform, mode
                )
        except asyncio.TimeoutError:
            return DownloadResult.fail(ErrorKind.TIMEOUT, "Download timed out")
        except Exception as exc:
            log.exception("downloader: unexpected error for %s", url)
            return DownloadResult.fail(ErrorKind.UNKNOWN, str(exc))

    # ------------------------------------------------------------------
    # Instagram via instaloader
    # ------------------------------------------------------------------

    def _get_instaloader(self) -> instaloader.Instaloader:
        if self._il is None:
            kwargs: dict = dict(
                download_videos=True,
                download_video_thumbnails=False,
                download_geotags=False,
                download_comments=False,
                save_metadata=False,
                compress_json=False,
                post_metadata_txt_pattern="",
                filename_pattern="{shortcode}",
                quiet=True,
            )
            if self._cfg.proxy:
                kwargs["proxies"] = {
                    "http":  self._cfg.proxy,
                    "https": self._cfg.proxy,
                }
            self._il = instaloader.Instaloader(**kwargs)

            # Attempt login if credentials are available
            if self._cfg.ig_username and self._cfg.ig_password and not self._il_logged_in:
                try:
                    self._il.login(self._cfg.ig_username, self._cfg.ig_password)
                    self._il_logged_in = True
                    log.info("downloader: instaloader logged in as %s", self._cfg.ig_username)
                except instaloader.exceptions.BadCredentialsException:
                    log.warning("downloader: Instagram login failed — bad credentials; using anonymous")
                except instaloader.exceptions.TwoFactorAuthRequiredException:
                    log.warning("downloader: Instagram 2FA required — using anonymous session")
                except Exception as exc:
                    log.warning("downloader: Instagram login error: %s — using anonymous", exc)

        return self._il

    def _download_instagram(
        self, url: str, work_dir: Path, mode: MediaMode
    ) -> DownloadResult:
        il = self._get_instaloader()

        try:
            shortcode = _extract_ig_shortcode(url)

            if shortcode:
                return self._download_ig_post(il, shortcode, work_dir, mode)

            # Profile URL — download recent posts
            username = _extract_ig_username(url)
            if username:
                return self._download_ig_profile(il, username, work_dir, mode)

            return DownloadResult.fail(ErrorKind.UNSUPPORTED_URL, f"Cannot parse Instagram URL: {url}")

        except instaloader.exceptions.PrivateProfileNotFollowedException:
            return DownloadResult.fail(ErrorKind.PRIVATE_CONTENT, "Private Instagram profile")
        except instaloader.exceptions.LoginRequiredException:
            return DownloadResult.fail(ErrorKind.COOKIE_MISSING, "Instagram login required for this content")
        except instaloader.exceptions.ProfileNotExistsException:
            return DownloadResult.fail(ErrorKind.NOT_FOUND, "Instagram profile not found")
        except instaloader.exceptions.PostChangedException:
            return DownloadResult.fail(ErrorKind.NOT_FOUND, "Instagram post not found or changed")
        except instaloader.exceptions.QueryReturnedNotFoundException:
            return DownloadResult.fail(ErrorKind.NOT_FOUND, "Instagram content not found")
        except instaloader.exceptions.TooManyRequestsException:
            return DownloadResult.fail(ErrorKind.RATE_LIMITED, "Instagram rate limited — try again later")
        except instaloader.exceptions.ConnectionException as exc:
            msg = str(exc).lower()
            if "401" in msg or "login" in msg:
                return DownloadResult.fail(ErrorKind.COOKIE_EXPIRED, str(exc))
            if "429" in msg or "rate" in msg:
                return DownloadResult.fail(ErrorKind.RATE_LIMITED, str(exc))
            return DownloadResult.fail(ErrorKind.UNKNOWN, str(exc))
        except Exception as exc:
            log.exception("downloader: instaloader error for %s", url)
            return DownloadResult.fail(ErrorKind.UNKNOWN, str(exc))

    def _download_ig_post(
        self,
        il: instaloader.Instaloader,
        shortcode: str,
        work_dir: Path,
        mode: MediaMode,
    ) -> DownloadResult:
        post = instaloader.Post.from_shortcode(il.context, shortcode)
        il.dirname_pattern = str(work_dir)

        before = set(work_dir.iterdir()) if work_dir.exists() else set()
        il.download_post(post, target=work_dir)
        after  = set(work_dir.iterdir())

        files = _filter_by_mode(list(after - before), mode)
        if not files:
            return DownloadResult.fail(ErrorKind.NO_MEDIA, "No media files downloaded")

        oversized = [f for f in files if f.stat().st_size > self._cfg.max_filesize_mb * 1024 * 1024]
        if oversized:
            for f in oversized:
                f.unlink(missing_ok=True)
            files = [f for f in files if f not in oversized]
            if not files:
                return DownloadResult.fail(
                    ErrorKind.FILE_TOO_LARGE,
                    f"All files exceed {self._cfg.max_filesize_mb} MB limit",
                )

        return DownloadResult.ok(sorted(files))

    def _download_ig_profile(
        self,
        il: instaloader.Instaloader,
        username: str,
        work_dir: Path,
        mode: MediaMode,
        max_posts: int = 1,
    ) -> DownloadResult:
        profile = instaloader.Profile.from_username(il.context, username)
        il.dirname_pattern = str(work_dir)

        collected: list[Path] = []
        for post in profile.get_posts():
            if len(collected) >= max_posts:
                break
            before = set(work_dir.iterdir()) if work_dir.exists() else set()
            il.download_post(post, target=work_dir)
            after  = set(work_dir.iterdir())
            collected.extend(after - before)

        files = _filter_by_mode(collected, mode)
        if not files:
            return DownloadResult.fail(ErrorKind.NO_MEDIA, "No media found on profile")

        return DownloadResult.ok(sorted(files))

    # ------------------------------------------------------------------
    # TikTok / Facebook / X via gallery-dl
    # ------------------------------------------------------------------

    def _download_gallery_dl(
        self, url: str, work_dir: Path, platform: str, mode: MediaMode
    ) -> DownloadResult:
        gdl_bin = shutil.which("gallery-dl")
        if not gdl_bin:
            return DownloadResult.fail(ErrorKind.UNKNOWN, "gallery-dl not found in PATH")

        # Build config JSON for this invocation
        cfg_data: dict = {
            "extractor": {
                "base-directory": str(work_dir),
                "no-mtime": True,
            }
        }

        # Inject platform cookie if available
        cookie_env_map = {
            "tiktok":   "COOKIE_TIKTOK",
            "facebook": "COOKIE_FACEBOOK",
            "x":        "COOKIE_X",
        }
        cookie_env = cookie_env_map.get(platform)
        if cookie_env and (cookie_val := os.environ.get(cookie_env)):
            cfg_data["extractor"][platform] = {"cookies": _parse_cookie_string(cookie_val)}

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, prefix="gdl_cfg_"
        ) as tmp:
            json.dump(cfg_data, tmp)
            tmp_path = tmp.name

        cmd = [gdl_bin, "--config", tmp_path, "--no-mtime", url]
        log.info("downloader: gallery-dl cmd: %s", " ".join(cmd))

        try:
            before = set(work_dir.rglob("*")) if work_dir.exists() else set()

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self._cfg.timeout_sec,
            )

            after = set(work_dir.rglob("*"))
            new_files = [p for p in (after - before) if p.is_file()]

            stderr = result.stderr.lower()

            if result.returncode != 0 and not new_files:
                if "429" in stderr or "rate" in stderr:
                    return DownloadResult.fail(ErrorKind.RATE_LIMITED, result.stderr[:500])
                if "404" in stderr or "not found" in stderr:
                    return DownloadResult.fail(ErrorKind.NOT_FOUND, result.stderr[:500])
                if "private" in stderr or "login" in stderr or "401" in stderr:
                    return DownloadResult.fail(ErrorKind.PRIVATE_CONTENT, result.stderr[:500])
                if "unsupported" in stderr:
                    return DownloadResult.fail(ErrorKind.UNSUPPORTED_URL, result.stderr[:500])
                return DownloadResult.fail(ErrorKind.UNKNOWN, result.stderr[:500])

            files = _filter_by_mode(new_files, mode)
            if not files:
                return DownloadResult.fail(ErrorKind.NO_MEDIA, "gallery-dl produced no media files")

            # Drop oversized
            max_bytes = self._cfg.max_filesize_mb * 1024 * 1024
            files = [f for f in files if f.stat().st_size <= max_bytes]
            if not files:
                return DownloadResult.fail(
                    ErrorKind.FILE_TOO_LARGE,
                    f"All files exceed {self._cfg.max_filesize_mb} MB limit",
                )

            return DownloadResult.ok(sorted(files))

        except subprocess.TimeoutExpired:
            return DownloadResult.fail(ErrorKind.TIMEOUT, f"gallery-dl timed out after {self._cfg.timeout_sec}s")
        except Exception as exc:
            log.exception("downloader: gallery-dl error for %s", url)
            return DownloadResult.fail(ErrorKind.UNKNOWN, str(exc))
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_IG_SHORTCODE_RE = re.compile(
    r"instagram\.com/(?:p|reel|tv)/([A-Za-z0-9_\-]+)/?", re.IGNORECASE
)
_IG_USERNAME_RE  = re.compile(
    r"instagram\.com/([A-Za-z0-9_.]+)/?(?:\?.*)?$", re.IGNORECASE
)
_MEDIA_EXTS = {
    ".mp4", ".mov", ".avi", ".mkv", ".webm",   # video
    ".jpg", ".jpeg", ".png", ".webp", ".gif",  # image
    ".heic", ".heif",
}


def _extract_ig_shortcode(url: str) -> Optional[str]:
    m = _IG_SHORTCODE_RE.search(url)
    return m.group(1) if m else None


def _extract_ig_username(url: str) -> Optional[str]:
    m = _IG_USERNAME_RE.search(url)
    if m:
        username = m.group(1)
        # Exclude known non-username path segments
        if username not in {"p", "reel", "tv", "stories", "explore", "accounts"}:
            return username
    return None


def _filter_by_mode(files: list, mode: MediaMode) -> List[Path]:
    video_exts = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
    image_exts = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif"}
    result = []
    for f in files:
        p = Path(f)
        ext = p.suffix.lower()
        if ext not in _MEDIA_EXTS:
            continue
        if mode == MediaMode.VIDEOS and ext not in video_exts:
            continue
        if mode == MediaMode.PHOTOS and ext not in image_exts:
            continue
        result.append(p)
    return result


def _safe_name(url: str) -> str:
    """Convert a URL into a safe directory name fragment."""
    parsed = urlparse(url)
    name = re.sub(r"[^a-zA-Z0-9_\-]", "_", parsed.netloc + parsed.path)
    return name[:60] + f"_{int(time.time())}"


def _parse_cookie_string(cookie_str: str) -> dict:
    """
    Parse a Netscape/header-style cookie string into a dict suitable for gallery-dl.
    Handles both 'name=value; name2=value2' and tab-separated Netscape format.
    """
    cookies: dict = {}
    for line in cookie_str.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Netscape format: domain \t flag \t path \t secure \t expiry \t name \t value
        parts = line.split("\t")
        if len(parts) >= 7:
            cookies[parts[5]] = parts[6]
            continue
        # Header format: name=value; name2=value2
        for pair in line.split(";"):
            if "=" in pair:
                k, _, v = pair.strip().partition("=")
                cookies[k.strip()] = v.strip()
    return cookies
