"""
handlers.py — All Telegram command, callback, and document handlers.

Key fixes vs original:
  • query.answer() is now called IMMEDIATELY at the top of handle_callback,
    before any I/O.  This prevents "Query is too old" BadRequest errors that
    occurred when the download blocked for several minutes waiting on 429 retries.
  • safe_answer() wrapper silently swallows already-expired query errors.
  • Cookie deduplication is applied on upload (via config._deduplicate_cookie_lines).
  • error_handler registered on Application to prevent unhandled exception noise.
  • NEW: A new download request CANCELS any in-progress run and starts fresh
    immediately.  Uses asyncio.Event cancellation token so the running loop
    detects the signal between profiles/URLs without killing the process.

Access model:
  • Private chat     → owner only
  • Group (not whitelisted) → owner sees "Activate This Group" button only
                              non-owner → complete silence
  • Group (whitelisted)     → anyone: /start /run /list /status /cookies
                              owner only: /add /remove /clear /cancel
                                          /allowgroup /denygroup /groups
  • Topics (forum groups)   → each username gets its own topic thread,
                              reused on subsequent runs
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes

import auth
from config import MIN_COOKIE_BYTES, _deduplicate_cookie_lines
from downloader import Downloader, ErrorKind, MediaMode

if TYPE_CHECKING:
    from config import Config
    from storage import CookieStore, GroupStore, ProfileStore, TopicStore

logger = logging.getLogger(__name__)

# ── Platform metadata ──────────────────────────────────────────────────────────

PLATFORM_EMOJI: dict[str, str] = {
    "instagram": "📸",
    "tiktok":    "🎵",
    "facebook":  "💙",
    "x":         "✖️",
}

TOPIC_COLORS: dict[str, int] = {
    "instagram": 0xFF93B2,
    "tiktok":    0xFB6F5F,
    "facebook":  0x6FB9F0,
    "x":         0xCB86DB,
}

# ── Markup builders ────────────────────────────────────────────────────────────

def _main_menu(is_owner: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📸 Photos",  callback_data="run:photos"),
            InlineKeyboardButton("🎬 Videos",  callback_data="run:videos"),
            InlineKeyboardButton("📦 Both",    callback_data="run:both"),
        ],
        [
            InlineKeyboardButton("📋 Profiles", callback_data="menu:list"),
            InlineKeyboardButton("📊 Status",   callback_data="menu:status"),
            InlineKeyboardButton("🍪 Cookies",  callback_data="menu:cookies"),
        ],
        [
            InlineKeyboardButton("🛑 Stop Download", callback_data="menu:cancel"),
        ],
    ]
    if is_owner:
        rows.append([
            InlineKeyboardButton("✅ Activate Group",  callback_data="grp:allow_here"),
            InlineKeyboardButton("🏘️ My Groups",       callback_data="grp:list"),
        ])
        rows.append([
            InlineKeyboardButton("🗑️ Remove a Group", callback_data="grp:remove_prompt"),
        ])
    return InlineKeyboardMarkup(rows)


def _whitelist_prompt() -> InlineKeyboardMarkup:
    """Shown to owner when they /start in a non-whitelisted group."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Activate This Group", callback_data="grp:allow_here"),
    ]])


def _back_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu:main"),
    ]])


# ── Text helpers ───────────────────────────────────────────────────────────────

def _esc(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def _split_message(text: str, limit: int = 4096) -> list[str]:
    """Split long Telegram messages on line boundaries when possible."""
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        cut = remaining.rfind("\n", 0, limit + 1)
        if cut <= 0:
            cut = limit

        chunk = remaining[:cut].rstrip("\n")
        if chunk.endswith("\\") and cut > 1:
            cut -= 1
            chunk = remaining[:cut].rstrip("\n")

        chunks.append(chunk or remaining[:limit])
        remaining = remaining[cut:].lstrip("\n")

    return chunks


async def _send(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    thread_id: int | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    """Send a MarkdownV2 message, optionally into a forum topic thread."""
    try:
        chunks = _split_message(text)
        for idx, chunk in enumerate(chunks):
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=ParseMode.MARKDOWN_V2,
                message_thread_id=thread_id,
                reply_markup=reply_markup if idx == len(chunks) - 1 else None,
            )
    except TelegramError as exc:
        logger.error("send failed chat=%d thread=%s: %s", chat_id, thread_id, exc)


async def _safe_answer(query) -> None:  # type: ignore[no-untyped-def]
    """
    Answer a callback query, silently ignoring errors if the query has
    already expired (Telegram's 10-minute callback window).
    """
    try:
        await query.answer()
    except BadRequest as exc:
        # "Query is too old" or "query id is invalid" — safe to ignore
        logger.debug("Callback query already expired (ignored): %s", exc)
    except TelegramError as exc:
        logger.warning("Could not answer callback query: %s", exc)


# ── BotHandlers ────────────────────────────────────────────────────────────────

class BotHandlers:

    def __init__(
        self,
        cfg: "Config",
        profiles: "ProfileStore",
        cookies: "CookieStore",
        groups: "GroupStore",
        topics: "TopicStore",
    ) -> None:
        self._cfg      = cfg
        self._profiles = profiles
        self._cookies  = cookies
        self._groups   = groups
        self._topics   = topics
        self._dl          = Downloader(cfg)
        self._running     = False
        # Cancellation token: .set() signals the active run loop to abort
        # between profiles/URLs.  Replaced with a fresh Event on every new run.
        self._cancel_event: asyncio.Event = asyncio.Event()
        self._run_start_lock = asyncio.Lock()
        # Strong reference to the running Task so we can await its teardown
        # before starting a replacement run.
        self._run_task: asyncio.Task | None = None

    # ── Access helpers ────────────────────────────────────────────────────────

    def _ok(self, update: Update, require_owner: bool = False) -> bool:
        return auth.check(update, require_owner=require_owner)

    def _user_is_owner(self, update: Update) -> bool:
        u = update.effective_user
        return u is not None and auth.is_owner(u.id)

    # ── /start & /help ────────────────────────────────────────────────────────

    async def cmd_start(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        chat     = update.effective_chat
        is_owner = self._user_is_owner(update)

        if chat is None:
            return

        if chat.type == ChatType.PRIVATE and not is_owner:
            logger.warning("Private /start rejected for non-owner.")
            return

        # ── Group access logic ─────────────────────────────────────────────
        if chat.type != ChatType.PRIVATE:
            is_allowed = auth.is_group_allowed(chat.id)

            if not is_owner and not is_allowed:
                return  # complete silence

            if is_owner and not is_allowed:
                await _send(
                    ctx, chat.id,
                    "⚙️ *Bot not activated in this group yet\\.*\n\n"
                    "Tap the button below to activate it\\. "
                    "After activation, all members can use the bot here\\.",
                    reply_markup=_whitelist_prompt(),
                )
                return

        # ── Full menu ──────────────────────────────────────────────────────
        total  = self._profiles.total_count()
        markup = _main_menu(is_owner)
        text   = (
            "🤖 *SayFalse Media Downloader*\n\n"
            "📥 *Quick Start:*\n"
            "① Send `instagram\\_profiles\\.txt` to import profiles\n"
            "② Send `instagram\\.com\\_cookies\\.txt` to add cookies \\(\\>2 KB\\)\n"
            "③ Tap *📸 Photos*, *🎬 Videos*, or *📦 Both* to download\n\n"
            f"📊 *Profiles queued:* {total}\n"
            "🌐 *Platforms:* Instagram · TikTok · Facebook · X\n\n"
            "⬇️ *Choose an action:*"
        )
        await _send(ctx, chat.id, text, reply_markup=markup)

    # ── Callback router ───────────────────────────────────────────────────────

    async def handle_callback(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query

        # ── FIX: Answer immediately to prevent "Query is too old" ──────────
        # Telegram requires a response within 10 minutes. Because downloads
        # can take longer (especially with 429 back-off), we must acknowledge
        # the tap *before* any blocking work.
        await _safe_answer(query)

        chat     = update.effective_chat
        data     = query.data or ""
        is_owner = self._user_is_owner(update)

        if chat is None:
            return

        if chat.type == ChatType.PRIVATE and not is_owner:
            logger.warning("Private callback rejected for non-owner.")
            return

        # ── Group access guard ─────────────────────────────────────────────
        if chat.type != ChatType.PRIVATE:
            is_allowed = auth.is_group_allowed(chat.id)
            if not is_allowed:
                if data == "grp:allow_here" and is_owner:
                    pass  # owner activating the group — fall through
                else:
                    return  # silence everything else

        # ── Route callbacks ────────────────────────────────────────────────
        if data.startswith("run:"):
            mode = MediaMode.from_str(data.split(":")[1])
            await self._execute_run(update, ctx, mode)

        elif data == "menu:list":
            await self._cb_list(update, ctx)

        elif data == "menu:status":
            await self._cb_status(update, ctx)

        elif data == "menu:cookies":
            await self._cb_cookies(update, ctx)

        elif data == "menu:cancel":
            await self._cb_cancel(update, ctx)

        elif data == "menu:main":
            await self.cmd_start(update, ctx)

        elif data == "grp:allow_here":
            if not is_owner:
                return
            await self._cb_allow_here(update, ctx)

        elif data == "grp:list":
            if not is_owner:
                return
            await self._cb_groups_list(update, ctx)

        elif data == "grp:remove_prompt":
            if not is_owner:
                return
            await self._cb_remove_group_prompt(update, ctx)

        elif data.startswith("grp:deny:"):
            if not is_owner:
                return
            try:
                gid = int(data.split(":")[2])
            except (IndexError, ValueError):
                return
            self._groups.deny(gid)
            await _send(
                ctx, chat.id,
                f"🗑️ Group `{_esc(str(gid))}` removed\\.",
                reply_markup=_back_button(),
            )

    # ── Inline callback implementations ───────────────────────────────────────

    async def _cb_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat     = update.effective_chat
        profiles = self._profiles.all()
        lines: list[str] = []

        for platform, urls in profiles.items():
            if not urls:
                continue
            em   = PLATFORM_EMOJI.get(platform, "📁")
            plat = self._cfg.platforms[platform]
            lines.append(f"*{em} {_esc(plat.label)}* \\({len(urls)}\\)")
            for url in urls:
                uname = Downloader._extract_username(url, plat) or url
                lines.append(f"  • `{_esc(uname)}`")

        total = self._profiles.total_count()
        if lines:
            lines.append(f"\n_Total: {total} profile\\(s\\) queued_")
            text = "\n".join(lines)
        else:
            text = (
                "📭 *No profiles queued\\.*\n\n"
                "Send `instagram\\_profiles\\.txt` to import, or use `/add`\\."
            )
        await _send(ctx, chat.id, text, reply_markup=_back_button())

    async def _cb_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat     = update.effective_chat
        profiles = self._profiles.all()
        cookies  = self._cookies.list_all()

        plat_count = sum(1 for v in profiles.values() if v)
        total      = self._profiles.total_count()
        state      = (
            "🔄 *Running* — download in progress"
            if self._running else
            "✅ *Idle* — ready to download"
        )

        cookie_status = []
        for name, size in cookies:
            kb   = size / 1024
            warn = " ⚠️" if size < MIN_COOKIE_BYTES else " ✅"
            cookie_status.append(f"  {warn} `{_esc(name)}` \\({kb:.1f} KB\\)")

        lines = [
            "📊 *Bot Status*\n",
            f"*State:* {state}",
            f"*Active platforms:* {plat_count}",
            f"*Profiles queued:* {total}",
            f"*Cookie files:* {len(cookies)}",
        ]
        if cookie_status:
            lines.append("")
            lines.append("*🍪 Cookies:*")
            lines.extend(cookie_status)

        await _send(ctx, chat.id, "\n".join(lines), reply_markup=_back_button())

    async def _cb_cookies(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat   = update.effective_chat
        stored = self._cookies.list_all()
        lines  = ["*🍪 Cookie Files*\n"]

        if stored:
            for name, size in stored:
                kb   = size / 1024
                warn = " ⚠️ _\\(too small — may fail\\)_" if size < MIN_COOKIE_BYTES else ""
                lines.append(f"✅ `{_esc(name)}` — {kb:.1f} KB{warn}")
        else:
            lines.append("_No cookies uploaded yet\\._\n")

        lines += [
            "",
            "*📤 How to export valid cookies:*",
            "1\\. Install *Cookie\\-Editor* in Chrome/Firefox",
            "2\\. Log into Instagram \\(or TikTok etc\\.\\) in browser",
            "3\\. Open Cookie\\-Editor → *Export* → *Netscape* format",
            "4\\. Save as `instagram\\.com\\_cookies\\.txt`",
            "5\\. Send file here \\(must be \\>2 KB\\)",
            "",
            "⚠️ _Files under 2 KB = missing session cookies_",
            "_= gallery\\-dl will get 429 / login errors_",
        ]
        await _send(ctx, chat.id, "\n".join(lines), reply_markup=_back_button())

    async def _cb_cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if not self._running:
            await _send(ctx, chat.id, "ℹ️ Nothing is currently running\\.", reply_markup=_back_button())
            return
        self._cancel_event.set()
        await _send(
            ctx, chat.id,
            "🛑 *Stop requested\\.* "
            "The current profile will finish, then the run stops\\.",
            reply_markup=_back_button(),
        )

    async def _cb_allow_here(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if chat.type == ChatType.PRIVATE:
            await _send(
                ctx, chat.id,
                "ℹ️ Tap *✅ Activate This Group* from *inside the group* you want to activate\\.",
                reply_markup=_back_button(),
            )
            return

        added = self._groups.allow(chat.id)
        if added:
            is_owner = self._user_is_owner(update)
            total    = self._profiles.total_count()
            markup   = _main_menu(is_owner)
            msg = (
                f"✅ *Group activated\\!*\n\n"
                f"Group ID: `{_esc(str(chat.id))}`\n"
                f"📊 Profiles queued: *{total}*\n\n"
                f"Members can now use the bot here\\. "
                f"Choose an action below:"
            )
            await _send(ctx, chat.id, msg, reply_markup=markup)
        else:
            await _send(
                ctx, chat.id,
                "ℹ️ This group is already activated\\.",
                reply_markup=_back_button(),
            )

    async def _cb_groups_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        ids  = self._groups.list_all()
        if not ids:
            await _send(
                ctx, chat.id,
                "📭 *No groups activated yet\\.*\n\n"
                "Add the bot to a group, send /start there as owner, "
                "then tap *✅ Activate This Group*\\.",
                reply_markup=_back_button(),
            )
            return
        lines = [f"*🏘️ Activated Groups* \\({len(ids)}\\)\n"]
        for gid in ids:
            lines.append(f"• `{_esc(str(gid))}`")
        await _send(ctx, chat.id, "\n".join(lines), reply_markup=_back_button())

    async def _cb_remove_group_prompt(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        chat = update.effective_chat
        ids  = self._groups.list_all()
        if not ids:
            await _send(ctx, chat.id, "No groups to remove\\.", reply_markup=_back_button())
            return
        rows = [
            [InlineKeyboardButton(f"🗑️ {gid}", callback_data=f"grp:deny:{gid}")]
            for gid in ids
        ]
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="menu:main")])
        await _send(
            ctx, chat.id,
            "*Select a group to remove:*",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    # ── /add ─────────────────────────────────────────────────────────────────

    async def cmd_add(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args or len(ctx.args) < 2:
            await _send(
                ctx, update.effective_chat.id,
                "📝 *Usage:* `/add <platform> <url>`\n\n"
                "Platforms: `instagram` `tiktok` `facebook` `x`\n\n"
                "*Example:*\n`/add instagram https://www\\.instagram\\.com/username`",
            )
            return

        platform = ctx.args[0].lower().strip()
        url      = ctx.args[1].strip().rstrip("/")

        if platform not in self._cfg.platforms:
            plat_list = _esc(", ".join(self._cfg.platforms))
            await _send(ctx, update.effective_chat.id, f"❌ Unknown platform\\. Valid: {plat_list}")
            return
        if not url.startswith("http"):
            await _send(ctx, update.effective_chat.id, "❌ URL must start with `http`\\.")
            return

        plat_cfg = self._cfg.platforms[platform]
        username = Downloader._extract_username(url, plat_cfg) or url
        added    = self._profiles.add(platform, url)
        em       = PLATFORM_EMOJI.get(platform, "📁")
        msg      = (
            f"✅ Added `{_esc(username)}` → *{em} {_esc(platform)}*"
            if added else
            "ℹ️ Already in queue\\."
        )
        await _send(ctx, update.effective_chat.id, msg)

    # ── /remove ───────────────────────────────────────────────────────────────

    async def cmd_remove(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args or len(ctx.args) < 2:
            await _send(ctx, update.effective_chat.id, "Usage: `/remove <platform> <url>`")
            return

        platform = ctx.args[0].lower().strip()
        url      = ctx.args[1].strip().rstrip("/")

        if platform not in self._cfg.platforms:
            await _send(ctx, update.effective_chat.id, "❌ Unknown platform\\.")
            return

        removed = self._profiles.remove(platform, url)
        await _send(
            ctx, update.effective_chat.id,
            "🗑️ Removed\\." if removed else "❌ URL not found\\.",
        )

    # ── /list ─────────────────────────────────────────────────────────────────

    async def cmd_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        await self._cb_list(update, ctx)

    # ── /clear ────────────────────────────────────────────────────────────────

    async def cmd_clear(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args:
            await _send(ctx, update.effective_chat.id, "Usage: `/clear <platform>`")
            return

        platform = ctx.args[0].lower().strip()
        if platform not in self._cfg.platforms:
            await _send(ctx, update.effective_chat.id, "❌ Unknown platform\\.")
            return

        count = self._profiles.clear(platform)
        em    = PLATFORM_EMOJI.get(platform, "📁")
        await _send(
            ctx, update.effective_chat.id,
            f"🗑️ Cleared {count} profile\\(s\\) from {em} *{_esc(platform)}*\\.",
        )

    # ── /allowgroup ───────────────────────────────────────────────────────────

    async def cmd_allowgroup(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if chat is None:
            return

        if not self._user_is_owner(update):
            return

        if ctx.args:
            try:
                gid   = int(ctx.args[0])
                added = self._groups.allow(gid)
                await _send(
                    ctx, chat.id,
                    f"✅ Group `{_esc(str(gid))}` activated\\."
                    if added else
                    f"ℹ️ Group `{_esc(str(gid))}` already active\\.",
                )
            except ValueError:
                await _send(ctx, chat.id, "❌ Invalid group ID \\(must be a number\\)\\.")
            return

        if chat.type == ChatType.PRIVATE:
            await _send(
                ctx, chat.id,
                "Use `/allowgroup <group\\_id>` or send this command from inside the group\\.",
            )
            return

        added = self._groups.allow(chat.id)
        await _send(
            ctx, chat.id,
            f"✅ This group \\(`{_esc(str(chat.id))}`\\) is now activated\\."
            if added else
            "ℹ️ Already activated\\.",
        )

    # ── /denygroup ────────────────────────────────────────────────────────────

    async def cmd_denygroup(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args:
            await _send(ctx, update.effective_chat.id, "Usage: `/denygroup <group\\_id>`")
            return
        try:
            gid     = int(ctx.args[0])
            removed = self._groups.deny(gid)
            await _send(
                ctx, update.effective_chat.id,
                f"🗑️ Group `{_esc(str(gid))}` removed\\."
                if removed else
                f"❌ Group `{_esc(str(gid))}` not found\\.",
            )
        except ValueError:
            await _send(ctx, update.effective_chat.id, "❌ Invalid group ID\\.")

    # ── /groups ───────────────────────────────────────────────────────────────

    async def cmd_groups(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        await self._cb_groups_list(update, ctx)

    # ── /status ───────────────────────────────────────────────────────────────

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        await self._cb_status(update, ctx)

    # ── /cancel ───────────────────────────────────────────────────────────────

    async def cmd_cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        await self._cb_cancel(update, ctx)

    # ── /cookies ──────────────────────────────────────────────────────────────

    async def cmd_cookies(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        await self._cb_cookies(update, ctx)

    # ── /run ──────────────────────────────────────────────────────────────────

    async def cmd_run(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        mode_str = ctx.args[0].lower() if ctx.args else "both"
        await self._execute_run(update, ctx, MediaMode.from_str(mode_str))

    # ── Core download executor ────────────────────────────────────────────────

    async def _execute_run(
        self,
        update: Update,
        ctx: ContextTypes.DEFAULT_TYPE,
        mode: MediaMode,
    ) -> None:
        chat = update.effective_chat
        if chat is None:
            return

        async with self._run_start_lock:
            # Stop the previous run before creating a new cancellation token.
            if self._running and self._run_task and not self._run_task.done():
                await _send(
                    ctx, chat.id,
                    "🔄 *Stopping previous download\\.* New run will start after the current profile finishes\\.",
                )
                self._cancel_event.set()
                try:
                    await asyncio.shield(self._run_task)
                except asyncio.CancelledError:
                    pass

            total = self._profiles.total_count()
            if total == 0:
                await _send(
                    ctx, chat.id,
                    "📭 *No profiles queued\\.*\n\n"
                    "Send `instagram\\_profiles\\.txt` to the chat, or use `/add`\\.",
                    reply_markup=_back_button(),
                )
                return

            # Fresh cancellation token for this run.
            cancel_event = asyncio.Event()
            self._cancel_event = cancel_event
            self._running      = True
            is_forum           = getattr(chat, "is_forum", False)

            run_task = asyncio.get_running_loop().create_task(
                self._run_download(ctx, chat, mode, is_forum, cancel_event)
            )
            self._run_task = run_task

        # Await the task here so the handler waits for completion (or cancel)
        try:
            await run_task
        except asyncio.CancelledError:
            pass  # Cancelled by a subsequent request — already handled inside

    # ── Actual download work (runs inside an asyncio Task) ────────────────────

    async def _run_download(
        self,
        ctx: ContextTypes.DEFAULT_TYPE,
        chat,
        mode: MediaMode,
        is_forum: bool,
        cancel_event: asyncio.Event,
    ) -> None:
        """
        Core download loop.  Runs as an asyncio Task so _execute_run can
        cancel it cleanly when a new request arrives.

        Cancellation is cooperative: cancel_event is checked between
        every profile and every URL.  The currently-running gallery-dl
        subprocess is allowed to finish its current file before stopping.
        """
        total = self._profiles.total_count()
        try:
            await _send(
                ctx, chat.id,
                f"🚀 *Download started*\n"
                f"Mode: *{_esc(mode.label())}* \\| Profiles: *{total}*\n\n"
                f"_This may take a while — Instagram can be slow\\.\\.\\._",
            )

            grand_total = 0

            for platform, urls in self._profiles.all().items():
                if not urls:
                    continue

                # ── Cooperative cancellation checkpoint ────────────────────
                if cancel_event.is_set():
                    await _send(ctx, chat.id, "🛑 *Run cancelled — new request received\\.*")
                    return

                plat_cfg = self._cfg.platforms[platform]
                em       = PLATFORM_EMOJI.get(platform, "📁")
                await _send(
                    ctx, chat.id,
                    f"📂 *{em} {_esc(plat_cfg.label)}* — processing {len(urls)} profile\\(s\\)\\.\\.\\.",
                )

                for url in urls:
                    # ── Cooperative cancellation checkpoint ────────────────
                    if cancel_event.is_set():
                        await _send(ctx, chat.id, "🛑 *Run cancelled — new request received\\.*")
                        return

                    username  = Downloader._extract_username(url, plat_cfg) or url
                    thread_id: int | None = None

                    if is_forum:
                        thread_id = await self._get_or_create_topic(
                            ctx, chat.id, platform, username
                        )

                    await _send(
                        ctx, chat.id,
                        f"⏳ Downloading `{_esc(username)}`\\.\\.\\.",
                        thread_id=thread_id,
                    )

                    result = await self._dl.download_user(url, plat_cfg, mode)

                    if result.skipped:
                        await _send(
                            ctx, chat.id,
                            f"⚠️ Skipped `{_esc(username)}`: _{_esc(result.skip_reason)}_",
                            thread_id=thread_id,
                        )
                        continue

                    # ── Report errors ──────────────────────────────────────
                    for sub in result.results:
                        if sub.error_kind == ErrorKind.NONE:
                            continue

                        sf = _esc(sub.subfolder)
                        un = _esc(username)

                        if sub.error_kind == ErrorKind.RATE_LIMITED:
                            cookie_path = self._cfg.cookies_dir / "instagram.com_cookies.txt"
                            if cookie_path.exists():
                                kb          = round(cookie_path.stat().st_size / 1024, 1)
                                cookie_note = f"Current cookie: *{_esc(str(kb))} KB*"
                                if kb < 2:
                                    cookie_note += " ⚠️ _\\(too small — definitely the cause\\)_"
                            else:
                                cookie_note = "Cookie file: *missing*"

                            msg = (
                                f"🚫 *Rate limited \\(429\\)* — `{un}` \\[{sf}\\]\n\n"
                                f"{cookie_note}\n\n"
                                f"*Fix:* Export fresh cookies \\(\\>5 KB\\) via "
                                f"*Cookie\\-Editor* in Chrome → *Netscape* format → "
                                f"send `instagram\\.com\\_cookies\\.txt` here\\."
                            )

                        elif sub.error_kind == ErrorKind.LOGIN:
                            msg = (
                                f"🔐 *Login required* — `{un}` \\[{sf}\\]\n\n"
                                f"Upload valid `instagram\\.com\\_cookies\\.txt` and retry\\."
                            )

                        elif sub.error_kind == ErrorKind.NOT_FOUND:
                            msg = (
                                f"❓ *Not found* — `{un}` \\[{sf}\\]\n\n"
                                f"Account may be deleted or renamed\\."
                            )

                        elif sub.error_kind == ErrorKind.PRIVATE:
                            msg = (
                                f"🔒 *Private account* — `{un}` \\[{sf}\\]\n\n"
                                f"Upload cookies from a followed account to access\\."
                            )

                        elif sub.error_kind == ErrorKind.GALLERY_DL:
                            msg = (
                                f"🔥 *gallery\\-dl not installed\\!*\n\n"
                                f"Check your Dockerfile — `gallery-dl` must be in PATH\\."
                            )

                        elif sub.error_kind == ErrorKind.NETWORK:
                            msg = (
                                f"🌐 *Network error* — `{un}` \\[{sf}\\]\n\n"
                                f"Transient connection issue\\. Will retry next run\\."
                            )

                        else:
                            err_preview = _esc((sub.error or "unknown error")[:200])
                            msg = f"⚠️ *Error* — `{un}` \\[{sf}\\]\n`{err_preview}`"

                        await _send(ctx, chat.id, msg, thread_id=thread_id)

                    new_count    = result.total_new
                    grand_total += new_count

                    await _send(
                        ctx, chat.id,
                        f"✅ `{_esc(username)}` — *{new_count}* new file\\(s\\) downloaded",
                        thread_id=thread_id,
                    )

                    # ── Deliver files ──────────────────────────────────────
                    all_new: list[Path] = [
                        f for sub in result.results for f in sub.new_files
                    ]
                    if all_new:
                        await self._deliver_files(ctx, chat.id, all_new, thread_id)

            await _send(
                ctx, chat.id,
                f"🏁 *All done\\!*\n\n"
                f"📥 *{grand_total}* new file\\(s\\) downloaded in total\\.",
                reply_markup=_back_button(),
            )

        except asyncio.CancelledError:
            # Task was hard-cancelled (should not normally happen — we use
            # the cooperative cancel_event instead, but handle it safely).
            logger.info("Download task hard-cancelled for chat=%d", chat.id)
            raise  # re-raise so asyncio can clean up the Task properly

        except Exception as exc:
            logger.exception("Unexpected error during run: %s", exc)
            await _send(
                ctx, chat.id,
                f"🔥 *Unexpected error:*\n`{_esc(str(exc)[:300])}`",
                reply_markup=_back_button(),
            )
        finally:
            if self._run_task is asyncio.current_task():
                self._running = False

    # ── Forum topic management ────────────────────────────────────────────────

    async def _get_or_create_topic(
        self,
        ctx: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        platform: str,
        username: str,
    ) -> int | None:
        stored = self._topics.get(chat_id, platform, username)
        if stored is not None:
            return stored

        em         = PLATFORM_EMOJI.get(platform, "📁")
        color      = TOPIC_COLORS.get(platform, 0x6FB9F0)
        topic_name = f"{em} {username}"[:128]

        try:
            topic     = await ctx.bot.create_forum_topic(
                chat_id=chat_id,
                name=topic_name,
                icon_color=color,
            )
            thread_id = topic.message_thread_id
            self._topics.set(chat_id, platform, username, thread_id)
            logger.info("Created topic '%s' thread_id=%d", topic_name, thread_id)
            return thread_id
        except BadRequest as exc:
            logger.warning("Could not create topic '%s': %s", topic_name, exc)
            return None
        except TelegramError as exc:
            logger.error("Topic creation error: %s", exc)
            return None

    # ── File delivery ─────────────────────────────────────────────────────────

    async def _deliver_files(
        self,
        ctx: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        files: list[Path],
        thread_id: int | None,
    ) -> None:
        cap   = self._cfg.max_send_files
        limit = self._cfg.max_file_size_mb
        sent  = 0

        for path in files:
            if sent >= cap:
                remaining = len(files) - sent
                if remaining > 0:
                    await _send(
                        ctx, chat_id,
                        f"ℹ️ {remaining} more file\\(s\\) saved to disk "
                        f"\\(cap of {cap} per run reached\\)\\.",
                        thread_id=thread_id,
                    )
                break

            try:
                size_mb = path.stat().st_size / (1024 * 1024)
            except OSError:
                continue

            if size_mb > limit:
                await _send(
                    ctx, chat_id,
                    f"⚠️ `{_esc(path.name)}` — {size_mb:.1f} MB "
                    f"\\(too large for Telegram, saved to disk only\\)\\.",
                    thread_id=thread_id,
                )
                continue

            ext = path.suffix.lstrip(".").lower()
            try:
                with path.open("rb") as fh:
                    if ext in self._cfg.video_exts:
                        await ctx.bot.send_video(
                            chat_id=chat_id,
                            video=fh,
                            message_thread_id=thread_id,
                        )
                    elif ext in self._cfg.photo_exts:
                        await ctx.bot.send_photo(
                            chat_id=chat_id,
                            photo=fh,
                            message_thread_id=thread_id,
                        )
                    else:
                        await ctx.bot.send_document(
                            chat_id=chat_id,
                            document=fh,
                            message_thread_id=thread_id,
                        )
                sent += 1
            except TelegramError:
                # Fallback: send as generic document
                try:
                    with path.open("rb") as fh:
                        await ctx.bot.send_document(
                            chat_id=chat_id,
                            document=fh,
                            message_thread_id=thread_id,
                        )
                    sent += 1
                except TelegramError as exc:
                    logger.warning("Could not send %s: %s", path.name, exc)

        logger.info(
            "Delivered %d/%d file(s) to chat=%d thread=%s",
            sent, len(files), chat_id, thread_id,
        )

    # ── Document upload handler ───────────────────────────────────────────────

    async def handle_document(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle cookie files and bulk profile imports uploaded as documents."""
        chat = update.effective_chat
        if not self._ok(update, require_owner=True):
            return

        doc  = update.message.document
        name = (doc.file_name or "").strip()
        if not name:
            return

        # ── Cookie file ────────────────────────────────────────────────────
        if self._cookies.is_valid_name(name):
            try:
                tg_file = await ctx.bot.get_file(doc.file_id)
                raw     = bytes(await tg_file.download_as_bytearray())

                if not raw:
                    await _send(ctx, chat.id, "❌ Received an empty file\\.")
                    return

                # Deduplicate cookie entries before saving
                raw = _deduplicate_cookie_lines(raw)

                size_warn = ""
                if len(raw) < MIN_COOKIE_BYTES:
                    size_warn = (
                        f"\n\n⚠️ *Warning:* Only {len(raw):,} bytes received\\. "
                        f"Valid cookies should be \\>2 KB\\. "
                        f"This file likely has missing session cookies — "
                        f"Instagram will return *429* or refuse auth\\.\n"
                        f"Re\\-export using *Cookie\\-Editor* \\(Netscape format\\)\\."
                    )

                self._cookies.save(name, raw)
                await _send(
                    ctx, chat.id,
                    f"🍪 *Cookie saved:* `{_esc(name)}`\n"
                    f"Size: {len(raw):,} bytes{size_warn}\n\n"
                    f"Tap below to start downloading\\.",
                    reply_markup=_main_menu(self._user_is_owner(update)),
                )
            except (TelegramError, OSError) as exc:
                logger.error("Cookie upload failed: %s", exc)
                await _send(ctx, chat.id, f"❌ Failed to save: `{_esc(str(exc)[:150])}`")
            return

        # ── Bulk profile import ────────────────────────────────────────────
        for platform in self._cfg.platforms:
            if name == f"{platform}_profiles.txt":
                try:
                    tg_file = await ctx.bot.get_file(doc.file_id)
                    raw     = bytes(await tg_file.download_as_bytearray())
                    text    = raw.decode(errors="replace")
                    urls    = [
                        line.strip().rstrip("/")
                        for line in text.splitlines()
                        if line.strip().startswith("http")
                    ]
                    if not urls:
                        await _send(ctx, chat.id, "❌ No valid URLs found in the file\\.")
                        return

                    added = self._profiles.add_bulk(platform, urls)
                    total = len(self._profiles.get(platform))
                    em    = PLATFORM_EMOJI.get(platform, "📁")
                    await _send(
                        ctx, chat.id,
                        f"📋 *Imported {added} profile\\(s\\)* into {em} *{_esc(platform)}*\n"
                        f"Total queued: *{total}*\n\n"
                        f"Tap below to start downloading\\.",
                        reply_markup=_main_menu(self._user_is_owner(update)),
                    )
                except (TelegramError, OSError) as exc:
                    logger.error("Bulk import failed: %s", exc)
                    await _send(ctx, chat.id, f"❌ Import failed: `{_esc(str(exc)[:150])}`")
                return

        await _send(
            ctx, chat.id,
            f"❓ *Unrecognised file:* `{_esc(name)}`\n\n"
            "*Expected filenames:*\n"
            "`instagram\\.com\\_cookies\\.txt`\n"
            "`tiktok\\.com\\_cookies\\.txt`\n"
            "`facebook\\.com\\_cookies\\.txt`\n"
            "`x\\.com\\_cookies\\.txt`\n"
            "`instagram\\_profiles\\.txt`\n"
            "`tiktok\\_profiles\\.txt` etc\\.",
        )


# ── Bot commands list (shown in Telegram "/" menu) ─────────────────────────────

BOT_COMMANDS: list[BotCommand] = [
    BotCommand("start",       "Show main menu"),
    BotCommand("run",         "Download [photos|videos|both]"),
    BotCommand("add",         "Add a profile: /add <platform> <url>"),
    BotCommand("remove",      "Remove a profile"),
    BotCommand("list",        "Show all queued profiles"),
    BotCommand("clear",       "Clear a platform queue"),
    BotCommand("status",      "Bot status and queue summary"),
    BotCommand("cookies",     "List uploaded cookie files"),
    BotCommand("cancel",      "Stop an ongoing download"),
    BotCommand("allowgroup",  "Activate bot in this group"),
    BotCommand("denygroup",   "Remove a group from whitelist"),
    BotCommand("groups",      "List all activated groups"),
]
