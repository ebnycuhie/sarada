"""
auth.py — Access control helpers.

Rules:
  Private chat  → owner only (always).
  Group chat    → group must be whitelisted.
  Management commands additionally require owner.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from telegram import Update
from telegram.constants import ChatType

if TYPE_CHECKING:
    from storage import GroupStore

logger = logging.getLogger(__name__)

_OWNER_ID: int = 0
_GROUP_STORE: "GroupStore | None" = None


def configure(owner_id: int, group_store: "GroupStore") -> None:
    global _OWNER_ID, _GROUP_STORE
    _OWNER_ID = owner_id
    _GROUP_STORE = group_store
    logger.info("Auth configured — owner_id=%d", owner_id)


def is_owner(user_id: int) -> bool:
    if _OWNER_ID == 0:
        logger.critical(
            "OWNER_ID not set; rejecting owner-only access. "
            "Set the OWNER_ID environment variable immediately."
        )
        return False
    return user_id == _OWNER_ID


def is_group_allowed(chat_id: int) -> bool:
    if _GROUP_STORE is None:
        return False
    return _GROUP_STORE.is_allowed(chat_id)


def check(update: Update, require_owner: bool = False) -> bool:
    """
    Central access-control gate. Returns True if the update is permitted.

    Private chat:
      - Always requires owner.
    Group / supergroup / forum:
      - Group must be whitelisted.
      - If require_owner=True, user must also be owner.
    """
    user = update.effective_user
    chat = update.effective_chat

    if user is None or chat is None:
        return False

    if chat.type == ChatType.PRIVATE:
        allowed = is_owner(user.id)
        if not allowed:
            logger.warning("PM rejected for user %d (%s)", user.id, user.username)
        return allowed

    # Group / supergroup / forum
    if not is_group_allowed(chat.id):
        logger.debug("Ignoring update from non-whitelisted group %d", chat.id)
        return False

    if require_owner and not is_owner(user.id):
        logger.warning(
            "Owner-only command rejected: user=%d group=%d",
            user.id, chat.id,
        )
        return False

    return True
