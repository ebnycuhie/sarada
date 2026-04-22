"""
auth.py — Owner-only access control decorator.

Usage:
    @owner_only
    async def cmd_run(update, ctx): ...
"""

from __future__ import annotations

import functools
import logging
from typing import Callable

from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

_OWNER_ID: int = 0


def configure(owner_id: int) -> None:
    """Call once at startup with the owner's Telegram user ID."""
    global _OWNER_ID
    _OWNER_ID = owner_id
    logger.info("Auth configured — owner_id=%d", owner_id)


def owner_only(handler: Callable) -> Callable:
    """
    Decorator that rejects all users except the configured owner.
    Silently ignores requests from unknown users (no reply) to avoid
    leaking that the bot exists to random callers.
    """
    @functools.wraps(handler)
    async def wrapper(
        update: Update,
        ctx: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        user = update.effective_user

        if user is None:
            logger.warning("Update with no user — rejected.")
            return

        if _OWNER_ID == 0:
            # OWNER_ID not configured — fail open with a warning so the
            # owner can still bootstrap, but log loudly.
            logger.critical(
                "OWNER_ID not set! Allowing user %d. Set OWNER_ID env var.",
                user.id,
            )
            await handler(update, ctx)
            return

        if user.id != _OWNER_ID:
            logger.warning(
                "Rejected user %d (%s) attempting '%s'",
                user.id,
                user.username or "no-username",
                update.message.text if update.message else "unknown",
            )
            # Silent reject — no response to the unauthorized user
            return

        await handler(update, ctx)

    return wrapper
