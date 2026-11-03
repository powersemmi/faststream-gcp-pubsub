from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

from faststream_gcp_pubsub.broker.config import PubSubBrokerConfig
from faststream_gcp_pubsub.message import PubSubRawMessage

from .registrator import PubSubRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable

    from faststream_gcp_pubsub.schemas import PubSub, PubSubTopic
    from faststream_gcp_pubsub.subscriber.settler import FlowControl


class PubSubPublisher(ArgsContainer):
    """Delayed publisher registration: the arguments of ``PubSubRegistrator.publisher``."""

    def __init__(
        self,
        topic: "str | PubSubTopic",
        *,
        ordering_key: str = "",
        headers: dict[str, str] | None = None,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            topic,
            ordering_key=ordering_key,
            headers=headers,
            persistent=persistent,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class PubSubRoute(SubscriberRoute):
    """Delayed subscriber registration: the arguments of ``PubSubRegistrator.subscriber``."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        subscription: "str | PubSub",
        *,
        topic: "str | PubSubTopic | None" = None,
        publishers: Iterable[PubSubPublisher] = (),
        flow_control: Optional["FlowControl"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Sequence["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        max_workers: int = 1,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call,
            subscription,
            publishers=publishers,
            persistent=persistent,
            topic=topic,
            flow_control=flow_control,
            ack_policy=ack_policy,
            no_reply=no_reply,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            codec=codec,
            max_workers=max_workers,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class PubSubRouter(
    PubSubRegistrator,
    BrokerRouter[PubSubRawMessage, PubSubBrokerConfig],
):
    """A group of subscribers and publishers to include into a ``PubSubBroker``.

    ``prefix`` is prepended to the topic and subscription names it declares.
    """

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[PubSubRoute] = (),
        *,
        dependencies: Sequence["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[PubSubRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
        ack_policy: AckPolicy = EMPTY,
    ) -> None:
        super().__init__(
            handlers=handlers,
            config=PubSubBrokerConfig(
                prefix=prefix,
                ack_policy=ack_policy,
                broker_dependencies=dependencies,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
            ),
            routers=routers,
        )
