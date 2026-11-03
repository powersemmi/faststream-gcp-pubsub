import asyncio
import logging
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest
from faststream import AckPolicy
from faststream.exceptions import IncorrectState

from faststream_gcp_pubsub import (
    DeadLetterPolicy,
    FlowControl,
    PubSubBroker,
    PubSubMessage,
    PubSubSubscription,
    PubSubTopic,
    RetryPolicy,
)
from tests.brokers.base.consume import BrokerRealConsumeTestcase

from .basic import PubSubTestcaseConfig
from .conftest import Admin


@pytest.mark.connected()
@pytest.mark.gcp()
@pytest.mark.asyncio()
class TestConsume(PubSubTestcaseConfig, BrokerRealConsumeTestcase):
    async def _drain(self, admin: Admin, subscription: str) -> list[Any]:
        response = await admin.subscriber.pull(
            subscription=admin.subscription_path(subscription),
            max_messages=10,
            return_immediately=True,
        )
        return list(response.received_messages)

    async def test_ack_is_sent_before_stop_returns(
        self, queue: str, admin: Admin, event: asyncio.Event
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(*self.get_subscriber_params(queue)[0])
        async def handler(m: Any) -> None:
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("hi", queue)
            await asyncio.wait_for(event.wait(), self.timeout)

        assert await self._drain(admin, queue) == []

    async def test_error_nacks_for_redelivery(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(
            PubSubSubscription(
                queue,
                declare=True,
                dead_letter_policy=DeadLetterPolicy(
                    queue + "-dlq", max_delivery_attempts=5
                ),
            )
        )
        async def handler(m: str, msg: PubSubMessage) -> None:
            mock(msg.delivery_attempt)
            if msg.delivery_attempt < 2:
                raise ValueError
            event.set()

        async with broker:
            await broker.start()
            await broker.declare_topic(queue + "-dlq")
            await broker.publish("hi", queue)
            await asyncio.wait_for(event.wait(), self.timeout)

        assert [c.args[0] for c in mock.call_args_list] == [1, 2]

    async def test_manual_nack_redelivers(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(
            *self.get_subscriber_params(queue)[0], ack_policy=AckPolicy.MANUAL
        )
        async def handler(m: str, msg: PubSubMessage) -> None:
            mock()
            if mock.call_count == 1:
                await msg.nack()
            else:
                await msg.ack()
                event.set()

        async with broker:
            await broker.start()
            await broker.publish("hi", queue)
            await asyncio.wait_for(event.wait(), self.timeout)

        assert mock.call_count == 2

    @pytest.mark.slow()
    async def test_lease_is_extended_past_ack_deadline(
        self, queue: str, admin: Admin, mock: MagicMock, event: asyncio.Event
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(
            PubSubSubscription(queue, declare=True, ack_deadline_seconds=10),
            flow_control=FlowControl(ack_deadline=10),
        )
        async def handler(m: Any) -> None:
            mock()
            # longer than the ack deadline: only the lease extension stops a redelivery
            await asyncio.sleep(13)
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("hi", queue)
            await asyncio.wait_for(event.wait(), 20)
            await asyncio.sleep(1)

        assert mock.call_count == 1
        assert await self._drain(admin, queue) == []

    async def test_ack_after_stop_raises(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(apply_types=True)
        kept: list[PubSubMessage] = []

        @broker.subscriber(
            *self.get_subscriber_params(queue)[0], ack_policy=AckPolicy.MANUAL
        )
        async def handler(m: str, msg: PubSubMessage) -> None:
            kept.append(msg)
            event.set()

        async with broker:
            await broker.start()
            await broker.publish("hi", queue)
            await asyncio.wait_for(event.wait(), self.timeout)

        with pytest.raises(IncorrectState):
            await kept[0].ack()

    async def test_declare_creates_subscription_with_settings(
        self, queue: str, admin: Admin
    ) -> None:
        broker = self.get_broker()
        sub = broker.subscriber(
            PubSubSubscription(
                queue,
                declare=True,
                ack_deadline_seconds=42,
                enable_message_ordering=True,
                filter='attributes.kind = "a"',
                message_retention_duration=timedelta(hours=3),
                retry_policy=RetryPolicy(
                    minimum_backoff=timedelta(seconds=20),
                    maximum_backoff=timedelta(seconds=40),
                ),
            ),
            topic=PubSubTopic(
                "t-" + queue, message_retention_duration=timedelta(hours=2)
            ),
        )

        @sub
        async def handler(m: Any) -> None: ...

        async with broker:
            await broker.start()

        created = await admin.subscriber.get_subscription(
            subscription=admin.subscription_path(queue)
        )
        assert created.topic == admin.topic_path("t-" + queue)
        assert created.ack_deadline_seconds == 42
        assert created.enable_message_ordering
        assert created.filter == 'attributes.kind = "a"'
        assert created.message_retention_duration == timedelta(hours=3)
        assert created.retry_policy.minimum_backoff == timedelta(seconds=20)
        assert created.retry_policy.maximum_backoff == timedelta(seconds=40)
        created_topic = await admin.publisher.get_topic(
            topic=admin.topic_path("t-" + queue)
        )
        assert created_topic.message_retention_duration == timedelta(hours=2)

    async def test_workers_handle_deliveries_at_once(self, queue: str) -> None:
        broker = self.get_broker()
        both_in_flight = asyncio.Event()
        in_flight = 0

        @broker.subscriber(*self.get_subscriber_params(queue)[0], max_workers=2)
        async def handler(m: Any) -> None:
            nonlocal in_flight
            in_flight += 1
            if in_flight == 2:
                both_in_flight.set()
            # one worker alone never lets the second delivery in
            await asyncio.wait_for(both_in_flight.wait(), self.timeout)

        async with broker:
            await broker.start()
            await broker.publish_batch("a", "b", topic=queue)
            await asyncio.wait_for(both_in_flight.wait(), self.timeout)

    async def test_lost_subscription_stops_and_is_logged(
        self, queue: str, admin: Admin
    ) -> None:
        logger = MagicMock()
        broker = self.get_broker(logger=logger)

        @broker.subscriber(*self.get_subscriber_params(queue)[0])
        async def handler(m: Any) -> None: ...

        async with broker:
            await broker.start()
            await admin.subscriber.delete_subscription(
                subscription=admin.subscription_path(queue)
            )
            with anyio.fail_after(self.timeout):
                while not any(  # ruff: ignore[async-busy-wait]
                    level == logging.ERROR
                    and text.startswith(f"Subscription {queue} stopped: NotFound")
                    for (level, text), _ in logger.log.call_args_list
                ):
                    await asyncio.sleep(0.1)


def test_flow_control_refuses_impossible_limits() -> None:
    with pytest.raises(ValueError, match="max_messages"):
        FlowControl(max_messages=0)
    with pytest.raises(ValueError, match="ack_deadline"):
        FlowControl(ack_deadline=9)
    with pytest.raises(ValueError, match="ack_deadline"):
        FlowControl(ack_deadline=601)


def test_workers_beyond_the_flow_limit_are_refused() -> None:
    broker = PubSubBroker(project_id="p")

    with pytest.raises(ValueError, match="max_workers=3 exceeds"):
        broker.subscriber(
            "orders", max_workers=3, flow_control=FlowControl(max_messages=2)
        )
