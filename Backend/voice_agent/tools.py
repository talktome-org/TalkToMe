"""
Function tools the voice agent can invoke during a conversation.

Tools call back into FastAPI over HTTP using a per-user identity. Start small:
right now the agent has no tools — voice is conversational. Add as the product
needs them (save a note, look up a recent message, set a reminder, etc.).
"""
from __future__ import annotations

from typing import Optional


def build_tools(user_id: Optional[str]) -> list:
    """
    Return a list of @function_tool-decorated callables scoped to `user_id`.
    Currently empty; expand here as we wire concrete capabilities.
    """
    return []
