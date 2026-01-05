"""
Backward-compatible re-exports.

New code should import from:
- `Backend.schemas.chat_models`
- `Backend.schemas.link_models`
- `Backend.schemas.partner_models`
"""

from .chat_models import (  # noqa: F401
    ChatHistoryMessage,
    ChatRequest,
    ChatResponse,
    MessageDTO,
    MessagesResponse,
    SessionDTO,
    SessionsResponse,
)
from .link_models import (  # noqa: F401
    AcceptLinkInviteRequest,
    AcceptLinkInviteResponse,
    CreateLinkInviteResponse,
    LinkStatusResponse,
    UnlinkResponse,
)
from .partner_models import (  # noqa: F401
    PartnerPendingRequestDTO,
    PartnerPendingRequestsResponse,
    PartnerRequestBody,
    PartnerRequestResponse,
)



