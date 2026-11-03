from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from typing_extensions import overload, override

from faststream_gcp_pubsub.broker.config import PubSubBrokerConfig
from faststream_gcp_pubsub.message import PubSubRawMessage
from faststream_gcp_pubsub.publisher.factory import create_publisher
from faststream_gcp_pubsub.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable

    from faststream_gcp_pubsub.publisher.usecase import PubSubPublisher
    from faststream_gcp_pubsub.schemas import PubSub, PubSubTopic
    from faststream_gcp_pubsub.subscriber.settler import FlowControl
    from faststream_gcp_pubsub.subscriber.usecase import (
        PubSubConcurrentSubscriber,
        PubSubDefaultSubscriber,
    )


class PubSubRegistrator(Registrator[PubSubRawMessage, PubSubBrokerConfig]):
    """Includable to PubSubBroker router."""

    @overload  # type: ignore[override]
    def subscriber(
        self,
        subscription: "str | PubSub",
        *,
        topic: "str | PubSubTopic | None" = None,
        flow_control: Optional["FlowControl"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Sequence["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        max_workers: Literal[1] = 1,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "PubSubDefaultSubscriber": ...

    @overload
    def subscriber(
        self,
        subscription: "str | PubSub",
        *,
        topic: "str | PubSubTopic | None" = None,
        flow_control: Optional["FlowControl"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Sequence["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        max_workers: int = ...,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "PubSubConcurrentSubscriber": ...

    @override
    def subscriber(
        self,
        subscription: "str | PubSub",
        *,
        topic: "str | PubSubTopic | None" = None,
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
    ) -> "PubSubDefaultSubscriber | PubSubConcurrentSubscriber":
        """Subscribe a handler to a Pub/Sub subscription.

        Args:
            subscription: the subscription to read, a name or a declaration.
            topic: the topic the subscription is attached to; the subscription's own name
                when omitted. It is created with the subscription when that is declared.
            flow_control: leasing, flow-control and ack-batching settings.
            ack_policy: when the delivery is acknowledged.
            no_reply: whether to skip publishing the handler result to ``reply_to``.
            dependencies: dependencies applied to the handler.
            parser: a parser to map deliveries to FastStream messages.
            decoder: a function to decode message bodies.
            codec: a codec object for the body.
            max_workers: deliveries handled at once.
            persistent: whether to keep the subscriber across broker restarts.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
            include_in_schema: whether to include the operation in the AsyncAPI schema.
        """
        subscriber = create_subscriber(
            subscription=subscription,
            topic=topic,
            flow_control=flow_control,
            ack_policy=ack_policy,
            no_reply=no_reply,
            config=cast("PubSubBrokerConfig", self.config),
            max_workers=max_workers,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            codec_=codec,
            dependencies_=dependencies,
        )

    @override
    def publisher(  # type: ignore[override]
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
    ) -> "PubSubPublisher":
        """Create a publisher object for a Pub/Sub topic.

        Args:
            topic: the topic to publish to, a name or a declaration.
            ordering_key: the ordering key of every message, unless a call sets another.
            headers: attributes added to every message.
            persistent: whether to keep the publisher across broker restarts.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type.
            include_in_schema: whether to include the operation in the AsyncAPI schema.
        """
        publisher = create_publisher(
            topic=topic,
            ordering_key=ordering_key,
            headers=headers,
            broker_config=cast("PubSubBrokerConfig", self.config),
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )
        super().publisher(publisher, persistent=persistent)
        return publisher

    @override
    def include_router(
        self,
        router: "PubSubRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Sequence["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        if not isinstance(router, PubSubRegistrator):
            msg = (
                f"Router must be an instance of PubSubRegistrator, "
                f"got {type(router).__name__} instead."
            )
            raise TypeError(msg)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
