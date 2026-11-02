"""``TestPubSubBroker``: the broker in process, with no Pub/Sub service behind it."""

import asyncio
import itertools
from collections import defaultdict
from collections.abc import AsyncIterator, Generator, Iterable, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast, overload
from uuid import uuid4

from faststream._internal.testing.broker import TestBroker, change_producer
from google.pubsub_v1.types import PullResponse, ReceivedMessage, StreamingPullResponse
from typing_extensions import override

from faststream_gcp_pubsub.broker.broker import PubSubBroker
from faststream_gcp_pubsub.limits import validate_publish_request, validate_resource_id
from faststream_gcp_pubsub.message import PubSubRawMessage
from faststream_gcp_pubsub.publisher.producer import PubSubProducer
from faststream_gcp_pubsub.schemas import PubSubSubscription, resource_path

if TYPE_CHECKING:
    from faststream_gcp_pubsub.publisher.usecase import PubSubPublisher
    from faststream_gcp_pubsub.response import PubSubPublishCommand
    from faststream_gcp_pubsub.subscriber.usecase import PubSubBaseSubscriber

__all__ = ("TestPubSubBroker",)

RawReceivedMessage: Any = ReceivedMessage.pb()


class TestPubSubBroker(TestBroker[PubSubBroker, Any], broker=PubSubBroker):
    """The broker in process: a publish reaches the subscriptions of its topic directly.

    It routes the way the service does. A topic delivers to every subscription attached to
    it; the consumers of one subscription share its messages; a subscription read by
    ``get_one`` or iteration keeps what is published until it is read. A publish the service
    rejects (attribute, size and name limits) raises the service's ``InvalidArgument``.

    Examples:
        >>> async with TestPubSubBroker(broker) as br:
        ...     await br.publish({"id": 1}, "orders")
        ...     handler.mock.assert_called_once_with({"id": 1})
    """

    @overload
    def __init__(
        self: "TestPubSubBroker",
        broker: PubSubBroker,
        /,
        *,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: "TestPubSubBroker",
        *brokers: PubSubBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None: ...

    def __init__(
        self,
        *brokers: PubSubBroker,
        with_real: bool = False,
        connect_only: bool | None = None,
    ) -> None:
        super().__init__(*brokers, with_real=with_real, connect_only=connect_only)
        self.service = InMemoryService()

    def create_publisher_fake_subscriber(
        self,
        broker: PubSubBroker,
        publisher: "PubSubPublisher",
    ) -> tuple["PubSubBaseSubscriber", bool]:
        project = broker.config.project_id
        topic = resource_path(project, "topics", publisher.topic)
        for b in self.brokers:
            for s in b.subscribers:
                sub = cast("PubSubBaseSubscriber", s)
                if resource_path(b.config.project_id, "topics", sub.topic) == topic:
                    return sub, True

        # Under `with_real` the subscription is created on the service only when the
        # publisher declares its topic: a test never adds resources nobody asked for.
        subscription = PubSubSubscription(
            publisher.topic, declare=publisher.topic_schema.declare
        )
        sub = cast(
            "PubSubBaseSubscriber", broker.subscriber(subscription, persistent=False)
        )
        return sub, False

    @contextmanager
    def _patch_producer(self, broker: PubSubBroker) -> Generator[None, None, None]:
        producer = InMemoryProducer(broker, self.brokers, self.service)
        with change_producer(broker.config.broker_config, producer):
            yield

    @override
    async def _fake_connect(  # type: ignore[override]
        self,
        broker: PubSubBroker,
        *args: Any,
        **kwargs: Any,
    ) -> "InMemoryConnection":
        # Subscribers started after the broker, and `get_one` or iteration, reach the
        # in-memory service through this connection.
        connection = InMemoryConnection(self.service)
        broker.config.broker_config._connection = connection  # type: ignore[assignment]
        return connection


class _NoopAcker:
    """Settles a message handed to a handler directly: there is no lease to return."""

    async def ack(self, message: PubSubRawMessage) -> None: ...

    async def nack(self, message: PubSubRawMessage) -> None: ...


_NOOP_ACKER = _NoopAcker()


class InMemoryService:
    """What a subscription keeps while no handler reads it."""

    def __init__(self) -> None:
        self.backlog: defaultdict[str, asyncio.Queue[Any]] = defaultdict(asyncio.Queue)
        self._ids = itertools.count(1)
        self._turn: defaultdict[str, int] = defaultdict(int)

    def next_message_id(self) -> str:
        return str(next(self._ids))

    def pick(self, subscription: str, consumers: Sequence[Any]) -> Any:
        """One consumer of a subscription per message, in turn."""
        turn = self._turn[subscription]
        self._turn[subscription] = turn + 1
        return consumers[turn % len(consumers)]


class InMemoryProducer(PubSubProducer):
    """Validates a publish like the service and delivers it in process."""

    def __init__(
        self,
        broker: PubSubBroker,
        brokers: Sequence[PubSubBroker],
        service: InMemoryService,
    ) -> None:
        super().__init__(parser=broker._parser, decoder=broker._decoder)
        self.broker = broker
        self.brokers = brokers
        self.service = service
        config = broker.config.broker_config
        self.project_id = config.project_id
        self.serializer = config.fd_config._serializer
        if config.broker_codec is not None:
            self.codec = config.broker_codec

    @property
    def subscribers(self) -> Iterable[tuple[str, "PubSubBaseSubscriber"]]:
        for b in self.brokers:
            for s in b.subscribers:
                yield b.config.project_id, cast("PubSubBaseSubscriber", s)

    @override
    async def _send(self, cmd: "PubSubPublishCommand") -> list[str]:
        request = await self.build_request(cmd)
        validate_publish_request(request)

        ids: list[str] = []
        for message in request.messages:
            message.message_id = self.service.next_message_id()
            message.publish_time.GetCurrentTime()
            ids.append(message.message_id)

        by_subscription: dict[str, list[PubSubBaseSubscriber]] = defaultdict(list)
        for project, sub in self.subscribers:
            if resource_path(project, "topics", sub.topic) == request.topic:
                by_subscription[
                    resource_path(project, "subscriptions", sub.subscription)
                ].append(sub)

        for path, consumers in by_subscription.items():
            handlers = [s for s in consumers if s.calls]
            for message in request.messages:
                received = RawReceivedMessage(ack_id=uuid4().hex)
                received.message.CopyFrom(message)
                if handlers:
                    sub = self.service.pick(path, handlers)
                    await sub.process_message(
                        PubSubRawMessage(received, path, _NOOP_ACKER)
                    )
                else:
                    self.service.backlog[path].put_nowait(received)
        return ids


class InMemoryConnection:
    """A connection whose clients serve the in-memory subscriptions."""

    def __init__(self, service: InMemoryService) -> None:
        self.publisher = _InMemoryPublisherClient()
        self.subscriber = _InMemorySubscriberClient(service)

    async def ready(self, timeout: float) -> bool:  # ruff: ignore[no-self-use]
        return True

    async def close(self) -> None: ...


class _InMemoryPublisherClient:
    async def create_topic(self, request: Any) -> None:  # ruff: ignore[no-self-use]
        # Topology is the service's concern, not the test's: a declaration always succeeds.
        validate_resource_id("topic", request.name)


class _StreamCall:
    """A streaming pull over one subscription's backlog."""

    def __init__(self, backlog: "asyncio.Queue[Any]") -> None:
        self._backlog = backlog

    def cancel(self) -> None: ...

    async def __aiter__(self) -> AsyncIterator[Any]:
        while True:
            received = await self._backlog.get()
            response = StreamingPullResponse.pb()()
            response.received_messages.append(received)
            yield StreamingPullResponse.wrap(response)


class _InMemorySubscriberClient:
    def __init__(self, service: InMemoryService) -> None:
        self._service = service

    async def create_subscription(self, request: Any) -> None:  # ruff: ignore[no-self-use]
        validate_resource_id("subscription", request.name)

    async def acknowledge(self, subscription: str, ack_ids: list[str]) -> None: ...

    async def modify_ack_deadline(
        self, subscription: str, ack_ids: list[str], ack_deadline_seconds: int
    ) -> None: ...

    async def pull(self, subscription: str, max_messages: int, timeout: float) -> Any:
        response = PullResponse.pb()()
        backlog = self._service.backlog[subscription]
        try:
            received = await asyncio.wait_for(backlog.get(), timeout)
        except asyncio.TimeoutError:
            return PullResponse.wrap(response)
        response.received_messages.append(received)
        return PullResponse.wrap(response)

    async def streaming_pull(self, requests: Any, metadata: Any) -> _StreamCall:
        _, path = metadata[0][1].split("=", 1)
        return _StreamCall(self._service.backlog[path])
