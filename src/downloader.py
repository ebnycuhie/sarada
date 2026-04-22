"""
downloader.py — Media download engine for the SayFalse bot.

Supports:
  • Instagram → instaloader  (avoids Railway datacenter 429 issues)
  • TikTok / Facebook / X   → gallery-dl
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import Config, PlatformConfig

logger = logging.getLogger(__name__)


# ── Enums ──────────────────────────────────────────────────────────────────────

class ErrorKind(Enum):
    NONE         = auto()
    RATE_LIMITED = auto()
    LOGIN        = auto()
    NOT_FOUND    = auto()
    PRIVATE      = auto()
    GALLERY_DL   = auto()
    NETWORK      = auto()
    TIMEOUT      = auto()
    UNKNOWN      = auto()


class MediaMode(Enum):
    PHOTOS = "photos"
    VIDEOS = "videos"
    BOTH   = "both"

    @classmethod
    def from_str(cls, value: str) -> "MediaMode":
        try:
            return cls(value.lower().strip())
        except ValueError:
            return cls.BOTH

    def label(self) -> str:
        return self.value.capitalize()


# ── Result dataclasses ─────────────────────────────────────────────────────────

@dataclass
class SubResult:
    subfolder:  str
    new_files:  list[Path] = field(default_factory=list)
    error_kind: ErrorKind  = ErrorKind.NONE
    error:      str | None = None


@dataclass
class DownloadResult:
    skipped:     bool        = False
    skip_reason: str         = ""
    results:     list[SubResult] = field(default_factory=list)

    @property
    def total_new(self) -> int:
        return sum(len(r.new_files) for r in self.results)


# ── Error classifier ───────────────────────────────────────────────────────────

def _classify_error(output: str, returncode: int) -> ErrorKind:
    low = output.lower()
    if "429" in output or "rate limit" in low or "too many requests" in low:
        return ErrorKind.RATE_LIMITED
    if "login" in low or "authentication" in low or "401" in output or "403" in output:
        return ErrorKind.LOGIN
    if "not found" in low or "404" in output or "does not exist" in low:
        return ErrorKind.NOT_FOUND
    if "private" in low:
        return ErrorKind.PRIVATE
    if returncode == 127:
        return ErrorKind.GALLERY_DL
    if "connection" in low or "timeout" in low or "network" in low:
        return ErrorKind.NETWORK
    return ErrorKind.UNKNOWN


# ── Downloader ─────────────────────────────────────────────────────────────────

class Downloader:

    def __init__(self, cfg: "Config") -> None:
        self._cfg = cfg
        logger.info(
            "downloader: Downloader initialised "
            "(Instagram=instaloader, others=gallery-dl)"
        )

    # ── Static helpers (called as Downloader._extract_username by handlers.py) ─

    @staticmethod
    def _extract_username(url: str, plat_cfg: "PlatformConfig") -> str | None:
        """
        Extract the bare username/handle from a profile URL.

        Examples:
          https://www.instagram.com/nasa/  → "nasa"
          https://www.tiktok.com/@nasa     → "nasa"
          https://x.com/NASA               → "NASA"
        """
        try:
            # Strip query-string, fragment, trailing slash
            clean = url.split("?")[0].split("#")[0].rstrip("/")
            prefix = plat_cfg.url_prefix.rstrip("/")

            if clean.startswith(prefix):
                rest = clean[len(prefix):].lstrip("/").lstrip("@")
                part = rest.split("/")[0]
                return part if part else None

            # Fallback: last non-empty URL path segment (strip leading @)
            parts = [p.lstrip("@") for p in clean.split("/") if p]
            return parts[-1] if parts else None
        except Exception:
            return None

    # ── Public async interface ─────────────────────────────────────────────────

    async def download_user(
        self,
        url: str,
        plat_cfg: "PlatformConfig",
        mode: MediaMode,
    ) -> DownloadResult:
        """Download all media for one profile URL and return a DownloadResult."""
        loop = asyncio.get_event_loop()
        if plat_cfg.name == "instagram":
            return await loop.run_in_executor(
                None, self._download_instagram, url, plat_cfg, mode
            )
        return await loop.run_in_executor(
            None, self._download_gallery_dl, url, plat_cfg, mode
        )

    # ── Instagram via instaloader ──────────────────────────────────────────────

    def _download_instagram(
        self,
        url: str,
        plat_cfg: "PlatformConfig",
        mode: MediaMode,
    ) -> DownloadResult:
        try:
            import instaloader
            import instaloader.exceptions as il_exc
        except ImportError:
            sub = SubResult(
                subfolder="Instagram",
                error_kind=ErrorKind.GALLERY_DL,
                error="instaloader is not installed; add it to requirements.txt",
            )
            return DownloadResult(results=[sub])

        username = self._extract_username(url, plat_cfg)
        if not username:
            return DownloadResult(
                skipped=True,
                skip_reason="Could not extract Instagram username from URL",
            )

        out_dir = self._cfg.base_dir / plat_cfg.folder / username
        out_dir.mkdir(parents=True, exist_ok=True)

        il = instaloader.Instaloader(
            dirname_pattern=str(out_dir),
            filename_pattern="{date_utc:%Y%m%d_%H%M%S}_{shortcode}",
            download_pictures=(mode in (MediaMode.PHOTOS, MediaMode.BOTH)),
            download_videos=(mode in (MediaMode.VIDEOS, MediaMode.BOTH)),
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            post_metadata_txt_pattern="",
            max_connection_attempts=3,
            quiet=True,
        )

        # Session / credential login
        ig_user = os.environ.get("INSTAGRAM_USERNAME", "").strip()
        ig_pass = os.environ.get("INSTAGRAM_PASSWORD", "").strip()
        session_file = self._cfg.cookies_dir / "instagram_session"

        if session_file.exists() and ig_user:
            try:
                il.load_session_from_file(ig_user, str(session_file))
                logger.debug("Instagram: loaded session from %s", session_file)
            except Exception as exc:
                logger.warning("Instagram: could not load session file: %s", exc)
        elif ig_user and ig_pass:
            try:
                il.login(ig_user, ig_pass)
                il.save_session_to_file(str(session_file))
                logger.info("Instagram: logged in as %s", ig_user)
            except il_exc.BadCredentialsException:
                sub = SubResult(
                    subfolder="Instagram",
                    error_kind=ErrorKind.LOGIN,
                    error="Bad Instagram credentials (INSTAGRAM_USERNAME/PASSWORD)",
                )
                return DownloadResult(results=[sub])
            except Exception as exc:
                logger.warning("Instagram: login attempt failed: %s", exc)

        before: set[Path] = set(out_dir.rglob("*"))

        # Resolve profile
        try:
            profile = instaloader.Profile.from_username(il.context, username)
        except il_exc.ProfileNotExistsException:
            sub = SubResult(
                subfolder="Instagram",
                error_kind=ErrorKind.NOT_FOUND,
                error=f"Instagram profile '{username}' does not exist",
            )
            return DownloadResult(results=[sub])
        except il_exc.PrivateProfileNotFollowedException:
            sub = SubResult(
                subfolder="Instagram",
                error_kind=ErrorKind.PRIVATE,
                error=f"Instagram profile '{username}' is private (not followed)",
            )
            return DownloadResult(results=[sub])
        except il_exc.ConnectionException as exc:
            err = str(exc)
            kind = ErrorKind.RATE_LIMITED if "429" in err else ErrorKind.NETWORK
            sub = SubResult(subfolder="Instagram", error_kind=kind, error=err)
            return DownloadResult(results=[sub])
        except Exception as exc:
            sub = SubResult(
                subfolder="Instagram",
                error_kind=ErrorKind.UNKNOWN,
                error=str(exc),
            )
            return DownloadResult(results=[sub])

        # Download posts
        try:
            il.download_profiles(
                {profile},
                tagged=False,
                igtv=False,
                highlights=False,
                stories=False,
                fast_update=True,
            )
        except il_exc.ConnectionException as exc:
            err = str(exc)
            kind = ErrorKind.RATE_LIMITED if "429" in err else ErrorKind.NETWORK
            sub = SubResult(subfolder="Instagram", error_kind=kind, error=err)
            return DownloadResult(results=[sub])
        except Exception as exc:
            sub = SubResult(
                subfolder="Instagram",
                error_kind=ErrorKind.UNKNOWN,
                error=str(exc),
            )
            return DownloadResult(results=[sub])

        after: set[Path] = set(out_dir.rglob("*"))
        new_files = self._filter_new_files(after - before, mode)
        sub = SubResult(
            subfolder="Instagram",
            new_files=new_files,
            error_kind=ErrorKind.NONE,
        )
        return DownloadResult(results=[sub])

    # ── gallery-dl  (TikTok / Facebook / X) ───────────────────────────────────

    def _download_gallery_dl(
        self,
        url: str,
        plat_cfg: "PlatformConfig",
        mode: MediaMode,
    ) -> DownloadResult:
        if not shutil.which("gallery-dl"):
            sub = SubResult(
                subfolder=plat_cfg.folder,
                error_kind=ErrorKind.GALLERY_DL,
                error="gallery-dl binary not found in PATH; check Dockerfile",
            )
            return DownloadResult(results=[sub])

        username = self._extract_username(url, plat_cfg)
        out_dir = self._cfg.base_dir / plat_cfg.folder / (username or "unknown")
        out_dir.mkdir(parents=True, exist_ok=True)

        cookie_path = self._cfg.cookies_dir / plat_cfg.cookie_file
        cmd: list[str] = ["gallery-dl", "--no-mtime"]

        if cookie_path.exists():
            cmd += ["--cookies", str(cookie_path)]

        if mode == MediaMode.PHOTOS:
            cmd += ["--filter", self._cfg.photo_filter]
        elif mode == MediaMode.VIDEOS:
            cmd += ["--filter", self._cfg.video_filter]

        cmd += [
            "--directory", str(out_dir),
            "--sleep", plat_cfg.sleep_sec,
            url,
        ]

        before: set[Path] = set(out_dir.rglob("*"))

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
            )
        except FileNotFoundError:
            sub = SubResult(
                subfolder=plat_cfg.folder,
                error_kind=ErrorKind.GALLERY_DL,
                error="gallery-dl not found (FileNotFoundError)",
            )
            return DownloadResult(results=[sub])
        except subprocess.TimeoutExpired:
            sub = SubResult(
                subfolder=plat_cfg.folder,
                error_kind=ErrorKind.TIMEOUT,
                error="gallery-dl timed out after 600 s",
            )
            return DownloadResult(results=[sub])

        combined = (proc.stdout or "") + (proc.stderr or "")

        if proc.returncode != 0:
            kind = _classify_error(combined, proc.returncode)
            sub = SubResult(
                subfolder=plat_cfg.folder,
                error_kind=kind,
                error=combined[:500].strip(),
            )
            return DownloadResult(results=[sub])

        after: set[Path] = set(out_dir.rglob("*"))
        new_files = self._filter_new_files(after - before, mode)
        sub = SubResult(
            subfolder=plat_cfg.folder,
            new_files=new_files,
            error_kind=ErrorKind.NONE,
        )
        return DownloadResult(results=[sub])

    # ── Internal file helpers ──────────────────────────────────────────────────

    def _filter_new_files(
        self,
        new_paths: set[Path],
        mode: MediaMode,
    ) -> list[Path]:
        """Return only real files from new_paths, filtered by media mode."""
        result: list[Path] = []
        for p in new_paths:
            if not p.is_file():
                continue
            ext = p.suffix.lstrip(".").lower()
            if mode == MediaMode.PHOTOS and ext not in self._cfg.photo_exts:
                continue
            if mode == MediaMode.VIDEOS and ext not in self._cfg.video_exts:
                continue
            result.append(p)
        return sorted(result)
