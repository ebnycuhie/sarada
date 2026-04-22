"""
downloader.py — gallery-dl subprocess wrapper that mirrors the .bat logic.

Mirrors these .bat behaviours exactly:
  • CHECK_AND_CLEAN_ARCHIVE  →  ArchiveCleaner.check_and_clean()
  • DOWNLOAD_USER / DU_BOTH  →  Downloader.download_user()
  • Per-platform sleep, cookies, folder structure
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator

if TYPE_CHECKING:
    from config import Config, PlatformConfig

logger = logging.getLogger(__name__)


# ── Types ──────────────────────────────────────────────────────────────────────

class MediaMode(Enum):
    PHOTOS = auto()
    VIDEOS = auto()
    BOTH   = auto()

    @classmethod
    def from_str(cls, s: str) -> "MediaMode":
        return {
            "photos": cls.PHOTOS,
            "videos": cls.VIDEOS,
            "both":   cls.BOTH,
        }.get(s.lower(), cls.BOTH)

    def label(self) -> str:
        return {
            MediaMode.PHOTOS: "Photos only",
            MediaMode.VIDEOS: "Videos only",
            MediaMode.BOTH:   "Both — Photos + Videos",
        }[self]


@dataclass
class SubfolderResult:
    subfolder: str          # "Photos" or "Videos"
    new_files: list[Path]   # files downloaded this run
    archive_action: str     # what the archive cleaner did
    error: str | None       # stderr from gallery-dl if any


@dataclass
class UserResult:
    username:  str
    platform:  str
    results:   list[SubfolderResult] = field(default_factory=list)
    skipped:   bool = False
    skip_reason: str = ""

    @property
    def total_new(self) -> int:
        return sum(len(r.new_files) for r in self.results)


# ── Archive cleaner ────────────────────────────────────────────────────────────

class ArchiveCleaner:
    """
    Mirrors :CHECK_AND_CLEAN_ARCHIVE from the .bat file.

    Logic:
      file_count == 0            → clear archive (folder empty)
      file_count < archive_count → clear archive (mismatch)
      else                       → archive valid, leave it
    """

    @staticmethod
    def check_and_clean(dl_dir: Path, archive: Path) -> str:
        """
        Inspects dl_dir and archive.txt.
        Returns a human-readable status string describing the action taken.
        """
        try:
            files = [
                f for f in dl_dir.iterdir()
                if f.is_file() and f.name != "archive.txt"
            ]
        except OSError:
            files = []

        file_count = len(files)

        arch_count = 0
        if archive.exists():
            try:
                arch_count = sum(1 for _ in archive.open(encoding="utf-8", errors="replace"))
            except OSError:
                arch_count = 0

        if file_count == 0:
            if archive.exists():
                try:
                    archive.unlink()
                    logger.debug("Archive cleared (folder empty): %s", archive)
                except OSError as exc:
                    logger.warning("Could not clear archive %s: %s", archive, exc)
            return "cleared — folder empty"

        if file_count < arch_count:
            try:
                archive.unlink()
                logger.debug(
                    "Archive cleared (mismatch %d files / %d entries): %s",
                    file_count, arch_count, archive,
                )
            except OSError as exc:
                logger.warning("Could not clear archive %s: %s", archive, exc)
            return f"cleared — mismatch ({file_count} files, {arch_count} archive entries)"

        return f"valid — {file_count} file(s) on disk, {arch_count} archive entries"


# ── gallery-dl runner ──────────────────────────────────────────────────────────

class GalleryDLRunner:
    """Thin async wrapper around gallery-dl CLI."""

    _GALLERY_DL = "gallery-dl"

    @staticmethod
    async def run(
        url:         str,
        dl_dir:      Path,
        ext_filter:  str,
        archive:     Path,
        cookies:     Path | None,
        sleep_sec:   str,
    ) -> tuple[int, str]:
        """
        Run gallery-dl and return (returncode, stderr_tail).
        stdout is intentionally discarded to avoid log flooding.
        """
        cmd = [
            GalleryDLRunner._GALLERY_DL,
            "--directory",        str(dl_dir),
            "--filter",           ext_filter,
            "--download-archive", str(archive),
            "--sleep-request",    sleep_sec,
            "--no-mtime",
        ]
        if cookies and cookies.exists():
            cmd += ["--cookies", str(cookies)]
        cmd.append(url)

        logger.debug("Executing: %s", " ".join(cmd))

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr_bytes = await proc.communicate()
        except FileNotFoundError:
            return -1, "gallery-dl not found in PATH"
        except OSError as exc:
            return -1, str(exc)

        stderr = (stderr_bytes or b"").decode(errors="replace").strip()

        if proc.returncode != 0:
            logger.warning(
                "gallery-dl exited %d for %s — %s",
                proc.returncode, url, stderr[-200:],
            )
        else:
            logger.info("gallery-dl OK for %s", url)

        # Return last 400 chars of stderr for display
        return proc.returncode, stderr[-400:] if stderr else ""


# ── Main download engine ───────────────────────────────────────────────────────

class Downloader:
    """
    Orchestrates per-user downloads across platforms.
    Mirrors the DOWNLOAD_USER / DU_BOTH / DU_PHOTOS / DU_VIDEOS
    subroutine structure from the .bat file.
    """

    def __init__(self, cfg: "Config") -> None:
        self._cfg     = cfg
        self._cleaner = ArchiveCleaner()

    def _snapshot(self, dl_dir: Path) -> set[Path]:
        """Return set of non-archive files currently in dl_dir."""
        try:
            return {
                f for f in dl_dir.iterdir()
                if f.is_file() and f.name != "archive.txt"
            }
        except OSError:
            return set()

    async def _download_subfolder(
        self,
        subfolder:  str,
        user_base:  Path,
        ext_filter: str,
        url:        str,
        plat:       "PlatformConfig",
        cookies:    Path | None,
    ) -> SubfolderResult:
        dl_dir  = user_base / subfolder
        archive = dl_dir / "archive.txt"

        try:
            dl_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return SubfolderResult(
                subfolder=subfolder,
                new_files=[],
                archive_action="error",
                error=f"Could not create directory: {exc}",
            )

        archive_action = ArchiveCleaner.check_and_clean(dl_dir, archive)
        before         = self._snapshot(dl_dir)

        returncode, stderr = await GalleryDLRunner.run(
            url=url,
            dl_dir=dl_dir,
            ext_filter=ext_filter,
            archive=archive,
            cookies=cookies,
            sleep_sec=plat.sleep_sec,
        )

        after     = self._snapshot(dl_dir)
        new_files = sorted(after - before, key=lambda p: p.stat().st_mtime)

        error: str | None = None
        if returncode != 0 and stderr:
            error = stderr

        return SubfolderResult(
            subfolder=subfolder,
            new_files=new_files,
            archive_action=archive_action,
            error=error,
        )

    async def download_user(
        self,
        url:   str,
        plat:  "PlatformConfig",
        mode:  MediaMode,
    ) -> UserResult:
        """
        Download photos and/or videos for one profile URL.
        Mirrors the bat's DOWNLOAD_USER subroutine.
        """
        username = self._extract_username(url, plat)
        if not username:
            return UserResult(
                username=url,
                platform=plat.name,
                skipped=True,
                skip_reason="Could not extract username from URL",
            )

        user_base = self._cfg.base_dir / plat.folder / username
        cookies   = self._cfg.cookies_dir / plat.cookie_file
        cookie_path = cookies if cookies.exists() else None

        if cookie_path is None:
            logger.warning("No cookie file for %s — proceeding without auth.", plat.name)

        result  = UserResult(username=username, platform=plat.name)
        tasks: list[tuple[str, str]] = []

        if mode in (MediaMode.PHOTOS, MediaMode.BOTH):
            tasks.append(("Photos", self._cfg.photo_filter))
        if mode in (MediaMode.VIDEOS, MediaMode.BOTH):
            tasks.append(("Videos", self._cfg.video_filter))

        for subfolder, ext_filter in tasks:
            sub_result = await self._download_subfolder(
                subfolder=subfolder,
                user_base=user_base,
                ext_filter=ext_filter,
                url=url,
                plat=plat,
                cookies=cookie_path,
            )
            result.results.append(sub_result)

        return result

    @staticmethod
    def _extract_username(url: str, plat: "PlatformConfig") -> str:
        """
        Strip the platform URL prefix to get the bare username.
        Mirrors the bat's:
          set "user=!url:https://www.instagram.com/=!"
          set "user=!user:/=!"
        """
        cleaned = url.strip().rstrip("/")
        # Remove prefix (case-insensitive for robustness)
        prefix_lower = plat.url_prefix.lower()
        if cleaned.lower().startswith(prefix_lower):
            cleaned = cleaned[len(plat.url_prefix):]
        cleaned = cleaned.lstrip("@").strip("/")
        # Reject if still looks like a full URL
        if cleaned.startswith("http"):
            return ""
        return cleaned
