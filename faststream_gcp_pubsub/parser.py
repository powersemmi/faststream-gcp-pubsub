from typing import TYPE_CHECKING

from faststream.message import decode_message

from .message import PubSubMessage

if TYPE_CHECKING:
    from faststream._internal.basic_types import DecodedMessage
    from faststream.message import StreamMessage

    from .message import PubSubRawMessage

__all__ = ("PubSubParser",)


class PubSubParser:
    """Turns a delivery into a FastStream message: attributes are its headers."""

    __slots__ = ()

    async def parse_message(self, message: "PubSubRawMessage") -> PubSubMessage:  # ruff: ignore[no-self-use]
        headers = message.attributes
        return PubSubMessage(
            raw_message=message,
            body=message.data,
            headers=headers,
            reply_to=headers.get("reply_to", ""),
            content_type=headers.get("content-type"),
            correlation_id=headers.get("correlation_id"),
            message_id=message.message_id,
        )

    async def decode_message(  # ruff: ignore[no-self-use]
        self, msg: "StreamMessage[PubSubRawMessage]"
    ) -> "DecodedMessage":
        return decode_message(msg)
