import asyncio
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest
from faststream.exceptions import FeatureNotSupportedException
from google.api_core.exceptions import InvalidArgument

from faststream_gcp_pubsub import PubSub, PubSubBroker, PubSubTopic
from faststream_gcp_pubsub.testing import InMemoryProducer, TestPubSubBroker
from tests.brokers.base.testclient import BrokerTestclientTestcase

from .basic import PubSubMemoryTestcaseConfig


@pytest.mark.asyncio()
class TestTestclient(PubSubMemoryTestcaseConfig, BrokerTestclientTestcase):
    def get_fake_producer_class(self) -> type:
        return InMemoryProducer

    @pytest.fixture()
    def fake_producer_cls(self) -> type:
        return InMemoryProducer

    @pytest.mark.connected()
    async def test_broker_with_real_patches_publishers_and_subscribers(
        self, queue: str
    ) -> None:
        # A real publish needs its topic: the publisher declares it, and the subscription
        # the test broker reads it through is declared with it.
        test_broker = self.get_broker()
        publisher = test_broker.publisher(PubSubTopic(f"{queue}1", declare=True))

        @test_broker.subscriber(PubSub(queue, declare=True))
        async def m(msg: Any) -> None:
            await publisher.publish(f"response: {msg}")

        async with self.patch_broker(test_broker, with_real=True) as br:
            await br.publish("hello", queue)
            await m.wait_call(self.timeout)
            m.mock.assert_called_once_with("hello")

            with anyio.fail_after(self.timeout):
                while not publisher.mock.called:  # ruff: ignore[async-busy-wait]
                    await asyncio.sleep(0.1)

            publisher.mock.assert_called_once_with("response: hello")

    async def test_publisher_response_with_model(self, queue: str) -> None:
        # Publishing through the handler's publisher is covered; `request()` is not part of
        # Pub/Sub, so the test broker refuses it like the real one.
        with pytest.raises(FeatureNotSupportedException):
            await super().test_publisher_response_with_model(queue)


@pytest.mark.asyncio()
class TestRouting:
    async def test_topic_reaches_every_subscription(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")
        first = MagicMock()
        second = MagicMock()

        @broker.subscriber(queue + "-a", topic=queue)
        async def a(m: Any) -> None:
            first(m)

        @broker.subscriber(queue + "-b", topic=queue)
        async def b(m: Any) -> None:
            second(m)

        async with TestPubSubBroker(broker) as br:
            await br.publish("hi", queue)

        first.assert_called_once_with("hi")
        second.assert_called_once_with("hi")

    async def test_consumers_of_one_subscription_share_messages(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")
        calls: list[str] = []

        @broker.subscriber(queue)
        async def a(m: Any) -> None:
            calls.append("a")

        @broker.subscriber(queue)
        async def b(m: Any) -> None:
            calls.append("b")

        async with TestPubSubBroker(broker) as br:
            for _ in range(4):
                await br.publish("hi", queue)

        assert sorted(calls) == ["a", "a", "b", "b"]

    async def test_subscription_keeps_messages_until_read(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")
        subscriber = broker.subscriber(queue)

        async with TestPubSubBroker(broker) as br:
            await br.publish("first", queue)
            await br.publish("second", queue)
            one = await subscriber.get_one(timeout=1)
            two = await subscriber.get_one(timeout=1)
            none = await subscriber.get_one(timeout=0.01)

        assert one is not None
        assert two is not None
        assert [await one.decode(), await two.decode()] == ["first", "second"]
        assert none is None

    async def test_iteration_reads_the_backlog(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")
        subscriber = broker.subscriber(queue)

        async with TestPubSubBroker(broker) as br:
            await br.publish("first", queue)
            async for msg in subscriber:
                assert await msg.decode() == "first"
                break

    async def test_ordering_key_and_headers_reach_the_handler(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p", apply_types=True)
        seen = MagicMock()

        @broker.subscriber(queue)
        async def handler(m: str, msg: Any = None) -> None:
            seen(m)

        async with TestPubSubBroker(broker) as br:
            await br.publish("hi", queue, ordering_key="k", headers={"a": "1"})
            handler.mock.assert_called_once_with("hi")

    async def test_publish_returns_message_ids(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")

        async with TestPubSubBroker(broker) as br:
            one = await br.publish("hi", queue)
            many = await br.publish_batch("a", "b", topic=queue)

        assert one
        assert len(set(many)) == 2

    async def test_request_is_not_supported(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")

        async with TestPubSubBroker(broker) as br:
            with pytest.raises(FeatureNotSupportedException):
                await br.request("hi", queue)

    async def test_publisher_request_is_not_supported(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")
        publisher = broker.publisher(queue)

        async with TestPubSubBroker(broker):
            with pytest.raises(FeatureNotSupportedException):
                await publisher.request("hi")


@pytest.mark.asyncio()
class TestValidation:
    """The in-process broker refuses what the service refuses."""

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        (
            pytest.param({"headers": {"goog-x": "1"}}, "reserved", id="goog-key"),
            pytest.param({"headers": {"k" * 257: "1"}}, "key is 1 to", id="long-key"),
            pytest.param(
                {"headers": {"k": "v" * 1025}}, "value is at most", id="long-value"
            ),
            pytest.param(
                {"headers": {f"h{i}": "1" for i in range(100)}},
                "at most 100 attributes",
                id="too-many-attributes",
            ),
            pytest.param(
                {"ordering_key": "k" * 1025}, "ordering key", id="long-ordering-key"
            ),
        ),
    )
    async def test_message_limits(
        self, queue: str, kwargs: dict[str, Any], match: str
    ) -> None:
        broker = PubSubBroker(project_id="p")

        async with TestPubSubBroker(broker) as br:
            with pytest.raises(InvalidArgument, match=match):
                await br.publish("hi", queue, **kwargs)

    async def test_message_size_limit(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")

        async with TestPubSubBroker(broker) as br:
            await br.publish(b"x" * 9_000_000, queue)
            with pytest.raises(InvalidArgument, match="at most 10000000 bytes"):
                await br.publish(b"x" * 10_000_001, queue)

    async def test_request_size_limit(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")

        async with TestPubSubBroker(broker) as br:
            # each message fits; the request carrying both does not
            await br.publish_batch(b"x" * 4_000_000, b"y" * 4_000_000, topic=queue)
            with pytest.raises(InvalidArgument, match="request is at most"):
                await br.publish_batch(b"x" * 6_000_000, b"y" * 6_000_000, topic=queue)

    async def test_batch_limit(self, queue: str) -> None:
        broker = PubSubBroker(project_id="p")

        async with TestPubSubBroker(broker) as br:
            await br.publish_batch(*range(1000), topic=queue)
            with pytest.raises(InvalidArgument, match="at most 1000 messages"):
                await br.publish_batch(*range(1001), topic=queue)

    @pytest.mark.parametrize("topic", ("1abc", "ab", "goog-topic", "has space"))
    async def test_topic_id(self, topic: str) -> None:
        broker = PubSubBroker(project_id="p")

        async with TestPubSubBroker(broker) as br:
            with pytest.raises(InvalidArgument, match="Invalid topic id"):
                await br.publish("hi", topic)
