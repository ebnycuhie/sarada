"""
handlers.py — All Telegram command and message handlers.

Every handler that mutates state or runs downloads is protected
by @owner_only. Public handlers (/start, /help) are open.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from auth import owner_only
from downloader import Downloader, MediaMode

if TYPE_CHECKING:
    from config import Config
    from storage import CookieStore, ProfileStore

logger = logging.getLogger(__name__)

# ── Telegram message helpers ───────────────────────────────────────────────────

async def _reply(update: Update, text: str) -> None:
    """Send a Markdown reply, truncating at Telegram's 4096-char limit."""
    try:
        await update.message.reply_text(
            text[:4096],
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except TelegramError as exc:
        logger.error("Failed to send reply: %s", exc)


def _esc(text: str) -> str:
    """Escape special MarkdownV2 characters."""
    for ch in r"_*[]()~`>#+-=|{}.!\\":
        text = text.replace(ch, f"\\{ch}")
    return text


async def _send_file(
    update: Update,
    path: Path,
    video_exts: frozenset[str],
    photo_exts: frozenset[str],
) -> bool:
    """
    Send a file to the chat.
    Returns True on success, False on failure.
    """
    ext = path.suffix.lstrip(".").lower()
    try:
        with path.open("rb") as fh:
            if ext in video_exts:
                await update.message.reply_video(fh)
            elif ext in photo_exts:
                await update.message.reply_photo(fh)
            else:
                await update.message.reply_document(fh)
        return True
    except TelegramError:
        # Fallback: send as document
        try:
            with path.open("rb") as fh:
                await update.message.reply_document(fh)
            return True
        except TelegramError as exc:
            logger.warning("Could not send %s: %s", path.name, exc)
            return False


# ── Handler class ──────────────────────────────────────────────────────────────

class BotHandlers:
    """
    Encapsulates all bot handlers with injected dependencies.
    Registered with the Application in bot.py.
    """

    def __init__(
        self,
        cfg:       "Config",
        profiles:  "ProfileStore",
        cookies:   "CookieStore",
    ) -> None:
        self._cfg      = cfg
        self._profiles = profiles
        self._cookies  = cookies
        self._dl       = Downloader(cfg)
        self._running  = False  # Simple re-entrancy guard

    # ── /start ────────────────────────────────────────────────────────────────

    async def cmd_start(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        lines = [
            "🤖 *Social Media Downloader*",
            "",
            "*Commands*",
            "`/add <platform> <url>` — queue a profile",
            "`/remove <platform> <url>` — remove a profile",
            "`/list` — show all queued profiles",
            "`/clear <platform>` — clear all URLs for a platform",
            "`/cookies` — list uploaded cookie files",
            "`/run [photos|videos|both]` — start downloading",
            "`/status` — show queue summary",
            "`/cancel` — stop ongoing download \\(best effort\\)",
            "",
            "*Platforms:* `instagram` · `tiktok` · `facebook` · `x`",
            "",
            "*Cookie upload:* send `instagram\\.com\\_cookies\\.txt` etc\\.",
            "*Bulk import:* send `instagram\\_profiles\\.txt` etc\\.",
        ]
        await update.message.reply_text(
            "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2
        )

    # ── /add ─────────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_add(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not ctx.args or len(ctx.args) < 2:
            await _reply(update, "Usage: `/add <platform> <url>`")
            return

        platform = ctx.args[0].lower().strip()
        url      = ctx.args[1].strip().rstrip("/")

        if platform not in self._cfg.platforms:
            plat_list = _esc(", ".join(self._cfg.platforms))
            await _reply(update, f"❌ Unknown platform\\. Use: {plat_list}")
            return

        if not url.startswith("http"):
            await _reply(update, "❌ URL must start with `http`")
            return

        added = self._profiles.add(platform, url)
        plat_cfg  = self._cfg.platforms[platform]
        username  = Downloader._extract_username(url, plat_cfg) or url

        if added:
            await _reply(
                update,
                f"✅ Added `{_esc(username)}` → *{_esc(platform)}*"
            )
        else:
            await _reply(update, "ℹ️ Already in queue\\.")

    # ── /remove ───────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_remove(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not ctx.args or len(ctx.args) < 2:
            await _reply(update, "Usage: `/remove <platform> <url>`")
            return

        platform = ctx.args[0].lower().strip()
        url      = ctx.args[1].strip().rstrip("/")

        if platform not in self._cfg.platforms:
            await _reply(update, "❌ Unknown platform\\.")
            return

        removed = self._profiles.remove(platform, url)
        if removed:
            await _reply(update, "🗑 Removed\\.")
        else:
            await _reply(update, "❌ URL not found in queue\\.")

    # ── /list ─────────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_list(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        profiles = self._profiles.all()
        lines: list[str] = []

        for platform, urls in profiles.items():
            if not urls:
                continue
            lines.append(f"*{_esc(platform.upper())}* \\({len(urls)}\\)")
            for url in urls:
                plat_cfg = self._cfg.platforms[platform]
                uname = Downloader._extract_username(url, plat_cfg) or url
                lines.append(f"  • `{_esc(uname)}`")

        if not lines:
            await _reply(update, "No profiles queued\\.")
            return

        total = self._profiles.total_count()
        lines.append(f"\n_Total: {total} profile\\(s\\)_")
        await _reply(update, "\n".join(lines))

    # ── /clear ────────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_clear(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not ctx.args:
            await _reply(update, "Usage: `/clear <platform>`")
            return

        platform = ctx.args[0].lower().strip()
        if platform not in self._cfg.platforms:
            await _reply(update, "❌ Unknown platform\\.")
            return

        count = self._profiles.clear(platform)
        await _reply(
            update,
            f"🗑 Cleared {count} profile\\(s\\) from *{_esc(platform)}*\\."
        )

    # ── /cookies ──────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_cookies(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        names = self._cookies.list_all()
        if not names:
            await _reply(update, "No cookies uploaded yet\\.")
            return

        lines = ["🍪 *Uploaded cookies:*", ""]
        for name in names:
            path = self._cookies.path_for(name)
            size_kb = path.stat().st_size / 1024
            lines.append(f"`{_esc(name)}` — {size_kb:.1f} KB")

        await _reply(update, "\n".join(lines))

    # ── /status ───────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_status(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        profiles = self._profiles.all()
        cookies  = self._cookies.list_all()

        plat_count = sum(1 for v in profiles.values() if v)
        total      = self._profiles.total_count()
        running    = "🔄 Download in progress" if self._running else "⏸ Idle"

        lines = [
            f"*Status:* {_esc(running)}",
            f"*Platforms active:* {plat_count}",
            f"*Total profiles queued:* {total}",
            f"*Cookies on disk:* {len(cookies)}",
        ]
        await _reply(update, "\n".join(lines))

    # ── /cancel ───────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_cancel(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not self._running:
            await _reply(update, "Nothing running\\.")
            return
        self._running = False
        await _reply(
            update,
            "⚠️ Cancel requested\\. Current download will finish then stop\\."
        )

    # ── /run ──────────────────────────────────────────────────────────────────

    @owner_only
    async def cmd_run(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if self._running:
            await _reply(
                update,
                "⚠️ A download is already running\\. Use `/cancel` first\\."
            )
            return

        mode_str = (ctx.args[0].lower() if ctx.args else "both")
        mode     = MediaMode.from_str(mode_str)
        total    = self._profiles.total_count()

        if total == 0:
            await _reply(
                update,
                "No profiles queued\\. Use `/add` or send a `*_profiles\\.txt` file\\."
            )
            return

        self._running = True

        try:
            await _reply(
                update,
                f"🚀 *Starting* \\[{_esc(mode.label())}\\]\n"
                f"_{total} profile\\(s\\) queued\\.\\.\\._"
            )

            grand_total_new = 0

            for platform, urls in self._profiles.all().items():
                if not urls:
                    continue
                if not self._running:
                    await _reply(update, "🛑 Download cancelled\\.")
                    return

                plat_cfg = self._cfg.platforms[platform]
                await _reply(update, f"\n📂 *{_esc(plat_cfg.label)}*")

                for url in urls:
                    if not self._running:
                        await _reply(update, "🛑 Download cancelled\\.")
                        return

                    result = await self._dl.download_user(url, plat_cfg, mode)

                    if result.skipped:
                        await _reply(
                            update,
                            f"⚠️ Skipped `{_esc(url)}`\n_{_esc(result.skip_reason)}_"
                        )
                        continue

                    # Report per-subfolder archive status
                    for sub in result.results:
                        status_line = (
                            f"  `\\[{_esc(sub.subfolder)}\\]` "
                            f"archive: {_esc(sub.archive_action)}"
                        )
                        if sub.error:
                            status_line += f"\n  ⚠️ `{_esc(sub.error[:200])}`"
                        await _reply(update, status_line)

                    new_count = result.total_new
                    grand_total_new += new_count

                    await _reply(
                        update,
                        f"✅ `{_esc(result.username)}` — {new_count} new file\\(s\\)"
                    )

                    # ── Send files to chat ──────────────────────────────────
                    all_new: list[Path] = [
                        f for sub in result.results for f in sub.new_files
                    ]
                    await self._deliver_files(update, all_new)

            await _reply(
                update,
                f"🏁 *All done* — {grand_total_new} new file\\(s\\) total\\."
            )

        except Exception as exc:
            logger.exception("Unexpected error in /run: %s", exc)
            await _reply(update, f"🔥 Internal error: `{_esc(str(exc)[:200])}`")
        finally:
            self._running = False

    async def _deliver_files(
        self, update: Update, files: list[Path]
    ) -> None:
        """Send new files to chat, respecting size/count caps."""
        cap   = self._cfg.max_send_files
        limit = self._cfg.max_file_size_mb

        sent    = 0
        skipped = 0

        for path in files:
            if sent >= cap:
                remaining = len(files) - sent - skipped
                if remaining > 0:
                    await _reply(
                        update,
                        f"ℹ️ {remaining} more file\\(s\\) saved to disk "
                        f"\\(cap of {cap} reached\\)\\."
                    )
                break

            try:
                size_mb = path.stat().st_size / (1024 * 1024)
            except OSError:
                skipped += 1
                continue

            if size_mb > limit:
                await _reply(
                    update,
                    f"⚠️ `{_esc(path.name)}` is {size_mb:.1f} MB — "
                    f"too large for Telegram \\(max {limit} MB\\), saved to disk\\."
                )
                skipped += 1
                continue

            ok = await _send_file(
                update, path,
                self._cfg.video_exts,
                self._cfg.photo_exts,
            )
            if ok:
                sent += 1
            else:
                skipped += 1

        logger.info("Delivered %d file(s), skipped %d", sent, skipped)

    # ── Document handler (cookies + bulk import) ──────────────────────────────

    @owner_only
    async def handle_document(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        doc  = update.message.document
        name = (doc.file_name or "").strip()

        if not name:
            return

        # ── Cookie file upload ───────────────────────────────────────────────
        if self._cookies.is_valid_name(name):
            try:
                tg_file = await ctx.bot.get_file(doc.file_id)
                raw     = await tg_file.download_as_bytearray()
                if not raw:
                    await _reply(update, "❌ Received empty file\\.")
                    return
                self._cookies.save(name, bytes(raw))
                await _reply(
                    update,
                    f"🍪 Cookie saved: `{_esc(name)}` \\({len(raw):,} bytes\\)"
                )
            except (TelegramError, OSError) as exc:
                logger.error("Cookie upload failed: %s", exc)
                await _reply(update, f"❌ Failed to save cookie: `{_esc(str(exc))}`")
            return

        # ── Bulk profile import ──────────────────────────────────────────────
        for platform in self._cfg.platforms:
            if name == f"{platform}_profiles.txt":
                try:
                    tg_file = await ctx.bot.get_file(doc.file_id)
                    raw     = await tg_file.download_as_bytearray()
                    text    = raw.decode(errors="replace")
                    urls = [
                        line.strip().rstrip("/")
                        for line in text.splitlines()
                        if line.strip().startswith("http")
                    ]
                    if not urls:
                        await _reply(update, "❌ No valid URLs found in file\\.")
                        return
                    added = self._profiles.add_bulk(platform, urls)
                    total = len(self._profiles.get(platform))
                    await _reply(
                        update,
                        f"📋 Added *{added}* profile\\(s\\) to "
                        f"*{_esc(platform)}* \\({total} total\\)"
                    )
                except (TelegramError, OSError) as exc:
                    logger.error("Bulk import failed: %s", exc)
                    await _reply(update, f"❌ Import failed: `{_esc(str(exc))}`")
                return

        await _reply(
            update,
            f"❓ Unrecognised file: `{_esc(name)}`\\.\n"
            "Expected `instagram\\.com\\_cookies\\.txt` or `instagram\\_profiles\\.txt` etc\\."
        )
