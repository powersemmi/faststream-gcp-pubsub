import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream_gcp_pubsub import PubSubMessage, PubSubResponse, PubSubSubscription

from .basic import PubSubTestcaseConfig
from .conftest import Admin

pytestmark = [pytest.mark.connected(), pytest.mark.gcp(), pytest.mark.asyncio()]


class TestFeatures(PubSubTestcaseConfig):
    async def test_ordering_key_keeps_order(
        self, queue: str, event: asyncio.Event
    ) -> None:
        broker = self.get_broker(apply_types=True)
        seen: list[int] = []

        @broker.subscriber(
            PubSubSubscription(queue, declare=True, enable_message_ordering=True)
        )
        async def handler(m: int, msg: PubSubMessage) -> None:
            assert msg.ordering_key == "k"
            seen.append(m)
            if len(seen) == 20:
                event.set()

        async with broker:
            await broker.start()
            publisher = broker.publisher(queue, ordering_key="k")
            for i in range(20):
                await publisher.publish(i)
            await asyncio.wait_for(event.wait(), self.timeout)

        assert seen == list(range(20))

    async def test_attributes_are_headers(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(*self.get_subscriber_params(queue)[0])
        async def handler(m: Any, msg: PubSubMessage) -> None:
            mock(
                msg.headers,
                msg.correlation_id,
                msg.content_type,
                msg.raw_message.publish_time,
            )
            event.set()

        before = datetime.now(timezone.utc).replace(tzinfo=None)
        async with broker:
            await broker.start()
            await broker.publish({"a": 1}, queue, headers={"x": "1"}, correlation_id="c")
            await asyncio.wait_for(event.wait(), self.timeout)

        headers, correlation_id, content_type, publish_time = mock.call_args.args
        assert headers == {
            "x": "1",
            "correlation_id": "c",
            "content-type": "application/json",
        }
        assert correlation_id == "c"
        assert content_type == "application/json"
        # a naive UTC time; the emulator keeps whole seconds
        after = datetime.now(timezone.utc).replace(tzinfo=None)
        assert before.replace(microsecond=0) <= publish_time <= after

    async def test_response_sets_reply_ordering_key(
        self, queue: str, admin: Admin
    ) -> None:
        broker = self.get_broker()
        out = queue + "-out"

        @broker.subscriber(*self.get_subscriber_params(queue)[0])
        @broker.publisher(out)
        async def handler(m: Any) -> PubSubResponse:
            return PubSubResponse("done", ordering_key="ok", headers={"h": "v"})

        await admin.create(out, out)
        pulled: Any = None
        async with broker:
            await broker.start()
            await broker.publish("go", queue)
            for _ in range(50):
                pulled = await admin.subscriber.pull(
                    subscription=admin.subscription_path(out), max_messages=1, timeout=5
                )
                if pulled.received_messages:
                    break

        [received] = pulled.received_messages
        assert received.message.data == b"done"
        assert received.message.ordering_key == "ok"
        assert received.message.attributes["h"] == "v"

    async def test_exactly_once_ack_is_confirmed(
        self, queue: str, admin: Admin, event: asyncio.Event
    ) -> None:
        broker = self.get_broker()

        sub = broker.subscriber(
            PubSubSubscription(queue, declare=True, enable_exactly_once_delivery=True)
        )

        @sub
        async def handler(m: Any) -> None:
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("once", queue)
            await asyncio.wait_for(event.wait(), self.timeout)
            assert sub._settler is not None
            assert sub._settler.exactly_once

        response = await admin.subscriber.pull(
            subscription=admin.subscription_path(queue),
            max_messages=10,
            return_immediately=True,
        )
        assert list(response.received_messages) == []
