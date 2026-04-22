"""
bot.py — Application entrypoint.

Bootstrap order:
  1. Load config  (raises RuntimeError if BOT_TOKEN is missing)
  2. Setup rotating logging
  3. Create storage instances
  4. Configure auth module
  5. Register all handlers + bot commands menu
  6. Start polling
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

import auth
import config as cfg_module
from config import Config
from handlers import BOT_COMMANDS, BotHandlers
from storage import CookieStore, GroupStore, ProfileStore, TopicStore


# ── Logging ────────────────────────────────────────────────────────────────────

def _setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fmt  = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    fh = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    for noisy in ("httpx", "httpcore", "urllib3", "telegram.vendor"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ── Startup hook ───────────────────────────────────────────────────────────────

async def _post_init(app: Application) -> None:
    """Register the bot commands menu that appears in Telegram's '/' popup."""
    try:
        await app.bot.set_my_commands(BOT_COMMANDS)
        logger.info("Bot commands menu registered (%d commands).", len(BOT_COMMANDS))
    except Exception as exc:
        logger.warning("Could not set bot commands: %s", exc)


# ── Wiring ─────────────────────────────────────────────────────────────────────

def _ensure_dirs(cfg: Config) -> None:
    for path in (
        cfg.base_dir,
        cfg.cookies_dir,
        cfg.profiles_file.parent,
        cfg.log_file.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)


def _build_app(cfg: Config) -> Application:
    profiles = ProfileStore(cfg)
    cookies  = CookieStore(cfg)
    groups   = GroupStore(cfg.profiles_file.parent)
    topics   = TopicStore(cfg.profiles_file.parent)

    auth.configure(cfg.owner_id, groups)

    h = BotHandlers(
        cfg=cfg,
        profiles=profiles,
        cookies=cookies,
        groups=groups,
        topics=topics,
    )

    app = (
        Application.builder()
        .token(cfg.bot_token)
        .post_init(_post_init)
        .build()
    )

    # ── Commands ───────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",      h.cmd_start))
    app.add_handler(CommandHandler("help",       h.cmd_start))
    app.add_handler(CommandHandler("add",        h.cmd_add))
    app.add_handler(CommandHandler("remove",     h.cmd_remove))
    app.add_handler(CommandHandler("list",       h.cmd_list))
    app.add_handler(CommandHandler("clear",      h.cmd_clear))
    app.add_handler(CommandHandler("run",        h.cmd_run))
    app.add_handler(CommandHandler("status",     h.cmd_status))
    app.add_handler(CommandHandler("cancel",     h.cmd_cancel))
    app.add_handler(CommandHandler("cookies",    h.cmd_cookies))
    app.add_handler(CommandHandler("allowgroup", h.cmd_allowgroup))
    app.add_handler(CommandHandler("denygroup",  h.cmd_denygroup))
    app.add_handler(CommandHandler("groups",     h.cmd_groups))

    # ── Inline button callbacks ────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(h.handle_callback))

    # ── File uploads (cookies + profile lists) ─────────────────────────────
    app.add_handler(MessageHandler(filters.Document.ALL, h.handle_document))

    return app


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        cfg = cfg_module.load()
    except RuntimeError as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(1)

    _setup_logging(cfg.log_file)

    logger.info("=" * 60)
    logger.info("Social Media Downloader Bot — starting")
    logger.info("Owner ID : %s", cfg.owner_id or "NOT SET ⚠️")
    logger.info("Base dir : %s", cfg.base_dir)
    logger.info("Platforms: %s", ", ".join(cfg.platforms))
    logger.info("=" * 60)

    if cfg.owner_id == 0:
        logger.critical(
            "OWNER_ID is not set — anyone can control this bot! "
            "Set the OWNER_ID environment variable immediately."
        )

    _ensure_dirs(cfg)

    # Log cookie status on startup
    for plat in cfg.platforms.values():
        cookie_path = cfg.cookies_dir / plat.cookie_file
        if cookie_path.exists():
            size = cookie_path.stat().st_size
            status = f"✅ {size:,} bytes"
            if size < 2000:
                status += " ⚠️  (too small — may cause 429)"
        else:
            status = "❌ missing"
        logger.info("Cookie [%s]: %s", plat.name, status)

    app = _build_app(cfg)
    logger.info("Polling for updates…")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query", "my_chat_member"],
    )


if __name__ == "__main__":
    main()
