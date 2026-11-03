from typing import TYPE_CHECKING, Any

from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, PublishCommand, Response
from typing_extensions import override

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage

__all__ = ("PubSubPublishCommand", "PubSubResponse")


class PubSubResponse(Response):
    """A handler result with Pub/Sub publish options.

    Return it from a handler to set the ordering key or attributes of the reply and of every
    publisher the handler is decorated with.
    """

    def __init__(
        self,
        body: "SendableMessage",
        *,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        ordering_key: str = "",
    ) -> None:
        super().__init__(body=body, headers=headers, correlation_id=correlation_id)
        self.ordering_key = ordering_key

    @override
    def as_publish_command(self) -> "PubSubPublishCommand":
        return PubSubPublishCommand(
            self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            ordering_key=self.ordering_key,
            _publish_type=PublishType.PUBLISH,
        )


class PubSubPublishCommand(BatchPublishCommand):
    """What a publish sends: bodies, topic, attributes and the ordering key."""

    def __init__(
        self,
        message: "SendableMessage",
        /,
        *messages: "SendableMessage",
        topic: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        reply_to: str = "",
        ordering_key: str = "",
        timeout: float | None = None,
        _publish_type: PublishType,
    ) -> None:
        super().__init__(
            message,
            *messages,
            destination=topic,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )
        self.ordering_key = ordering_key
        self.timeout = timeout

    @classmethod
    def from_cmd(
        cls,
        cmd: "PublishCommand | PubSubPublishCommand",
        *,
        batch: bool = False,
    ) -> "PubSubPublishCommand":
        if isinstance(cmd, PubSubPublishCommand):
            return cmd

        bodies = cmd.batch_bodies if batch else (cmd.body,)
        if not bodies:
            bodies = (None,)
        return cls(
            *bodies,
            topic=cmd.destination,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            ordering_key=getattr(cmd, "ordering_key", ""),
            timeout=getattr(cmd, "timeout", None),
            _publish_type=cmd.publish_type,
        )

    def __repr__(self) -> str:
        body = [f"body='{self.body}'"]
        if self.extra_bodies:
            body.append(f"extra_bodies={self.extra_bodies}")
        body.append(f"topic='{self.destination}'")
        if self.ordering_key:
            body.append(f"ordering_key='{self.ordering_key}'")
        if self.reply_to:
            body.append(f"reply_to='{self.reply_to}'")
        body.extend((
            f"headers={self.headers}",
            f"correlation_id='{self.correlation_id}'",
        ))
        return f"{self.__class__.__name__}({', '.join(body)})"
