import contextlib
import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import anyio
from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.utils import process_msg
from google.api_core import exceptions
from typing_extensions import override

from faststream_gcp_pubsub.message import PubSubRawMessage
from faststream_gcp_pubsub.parser import PubSubParser
from faststream_gcp_pubsub.publisher.fake import PubSubFakePublisher
from faststream_gcp_pubsub.schemas import PubSubTopic, resource_path

from .settler import Settler
from .stream import StreamingPull

if TYPE_CHECKING:
    from collections.abc import Iterable

    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage

    from faststream_gcp_pubsub.broker.config import PubSubBrokerConfig
    from faststream_gcp_pubsub.message import PubSubMessage

    from .config import PubSubSubscriberConfig


class PubSubBaseSubscriber(TasksMixin, SubscriberUsecase[PubSubRawMessage]):
    """Reads one Pub/Sub subscription."""

    __slots__ = ("_flow", "_settler", "_stream", "_subscription", "_topic")

    _outer_config: "PubSubBrokerConfig"

    def __init__(
        self,
        config: "PubSubSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[PubSubRawMessage]",
    ) -> None:
        parser = PubSubParser()
        config.parser = parser.parse_message
        config.decoder = parser.decode_message
        super().__init__(config, specification, calls)

        self._subscription = config.subscription
        self._topic = config.topic
        self._flow = config.flow_control
        self._settler: Settler | None = None
        self._stream: StreamingPull | None = None

    @property
    def subscription(self) -> str:
        """The subscription name, router prefix included."""
        return f"{self._outer_config.prefix}{self._subscription.name}"

    @property
    def topic(self) -> str:
        """The topic the subscription is attached to, router prefix included."""
        return f"{self._outer_config.prefix}{self._topic.name}"

    @property
    def path(self) -> str:
        """The full resource name of the subscription."""
        return resource_path(
            self._outer_config.project_id, "subscriptions", self.subscription
        )

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        return (
            PubSubFakePublisher(
                producer=self._outer_config.producer,
                topic=message.reply_to,
            ),
        )

    @staticmethod
    def build_log_context(
        message: "StreamMessage[Any] | None",
        subscription: str = "",
    ) -> dict[str, str]:
        return {
            "subscription": subscription,
            "message_id": getattr(message, "message_id", ""),
        }

    def get_log_context(
        self,
        message: "StreamMessage[Any] | None",
    ) -> dict[str, str]:
        return self.build_log_context(message=message, subscription=self.subscription)

    def _log_with_context(self, message: str, level: int) -> None:
        self._outer_config.logger.log(message, level, extra=self.get_log_context(None))

    def _make_settler(self) -> Settler:
        client = self._outer_config.connection.subscriber
        path = self.path

        async def send_ack(ids: "Iterable[str]") -> None:
            await client.acknowledge(subscription=path, ack_ids=list(ids))

        async def extend(ids: "Iterable[str]", seconds: int) -> None:
            await client.modify_ack_deadline(
                subscription=path, ack_ids=list(ids), ack_deadline_seconds=seconds
            )

        settler = Settler(
            flow=self._flow, send_ack=send_ack, extend=extend, log=self._log_with_context
        )
        if self._subscription.enable_exactly_once_delivery:
            # known before the stream reports it, so the first acks already wait for it
            settler.exactly_once = True
        return settler

    def _ensure_settler(self) -> Settler:
        if self._settler is None:
            self._settler = self._make_settler()
            self._settler.start()
        return self._settler

    async def declare(self) -> None:
        """Create the topic and the subscription when the declaration asks for it."""
        if not self._subscription.declare:
            return
        config = self._outer_config
        project = config.project_id
        # router prefixes are part of the names the service sees
        topic = replace(self._topic, name=self.topic)
        subscription = replace(self._subscription, name=self.subscription)
        topics = [topic]
        if (policy := subscription.dead_letter_policy) is not None:
            # the service refuses a dead-letter policy whose topic does not exist yet
            topics.append(PubSubTopic(policy.topic))
        for declared in topics:
            with contextlib.suppress(exceptions.AlreadyExists):
                await config.connection.publisher.create_topic(
                    request=declared.to_proto(project)
                )
        with contextlib.suppress(exceptions.AlreadyExists):
            await config.connection.subscriber.create_subscription(
                request=subscription.to_proto(project, topic.path(project))
            )

    @override
    async def start(self) -> None:
        await super().start()

        # a subscriber read by get_one() or iteration needs its subscription as well
        await self.declare()

        if self.calls:
            settler = self._ensure_settler()
            self._stream = StreamingPull(
                self._outer_config.connection.subscriber,
                self.path,
                settler,
                self._log_with_context,
            )
            self.add_task(self._run_consume_loop, restart_on_failure=False)

        self._post_start()

    @override
    async def stop(self) -> None:
        # Stop reading first: the stream is cancelled, a delivery in a handler is not, and the
        # base class waits for it up to the graceful timeout.
        self.running = False
        if self._stream is not None:
            self._stream.stop()
            self._stream = None

        await super().stop()

        if self._settler is not None:
            await self._settler.close()
            self._settler = None

    def _wrap(self, received: Any, settler: Settler) -> PubSubRawMessage:
        return PubSubRawMessage(received, self.path, settler)

    async def _run_consume_loop(self) -> None:
        try:
            await self._consume_loop()
        except Exception as e:
            self._log_with_context(
                f"Subscription {self.subscription} stopped: {e!r}", logging.ERROR
            )
            raise

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "PubSubMessage | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )
        settler = self._ensure_settler()
        client = self._outer_config.connection.subscriber

        raw_msg: PubSubRawMessage | None = None
        with anyio.move_on_after(timeout):
            while raw_msg is None:
                try:
                    response = await client.pull(
                        subscription=self.path, max_messages=1, timeout=timeout
                    )
                except exceptions.DeadlineExceeded:
                    break
                for received in type(response).pb(response).received_messages:
                    await settler.lease(received.ack_id)
                    raw_msg = self._wrap(received, settler)

        async_parser, async_decoder = self._get_parser_and_decoder()
        context = self._outer_config.context
        msg: PubSubMessage | None = await process_msg(  # type: ignore[assignment]
            msg=raw_msg,
            middlewares=(m(raw_msg, context=context) for m in self._broker_middlewares),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["PubSubMessage"]:
        settler = self._ensure_settler()
        stream = StreamingPull(
            self._outer_config.connection.subscriber,
            self.path,
            settler,
            self._log_with_context,
        )
        context = self._outer_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()
        try:
            async for received in stream:
                raw_msg = self._wrap(received, settler)
                msg: PubSubMessage = await process_msg(  # type: ignore[assignment]
                    msg=raw_msg,
                    middlewares=(
                        m(raw_msg, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                )
                yield msg
        finally:
            stream.stop()

    @abstractmethod
    async def _consume_loop(self) -> None:
        raise NotImplementedError


class PubSubDefaultSubscriber(PubSubBaseSubscriber):
    """Handles one delivery at a time."""

    __slots__ = ()

    async def _consume_loop(self) -> None:
        stream, settler = self._stream, self._settler
        assert stream is not None
        assert settler is not None
        async for received in stream:
            await self.consume(self._wrap(received, settler))


class PubSubConcurrentSubscriber(ConcurrentMixin[PubSubRawMessage], PubSubBaseSubscriber):
    """Handles up to ``max_workers`` deliveries at once."""

    __slots__ = ()

    @override
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def _consume_loop(self) -> None:
        stream, settler = self._stream, self._settler
        assert stream is not None
        assert settler is not None
        async for received in stream:
            await self._put_msg(self._wrap(received, settler))
