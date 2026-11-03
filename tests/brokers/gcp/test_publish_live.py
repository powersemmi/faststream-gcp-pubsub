import pytest
from google.api_core.exceptions import NotFound

from faststream_gcp_pubsub import PubSubBroker

from .conftest import Admin, Settings

pytestmark = [pytest.mark.gcp, pytest.mark.asyncio]


async def test_publish_reaches_subscription(
    settings: Settings, admin: Admin, topic: str, subscription: str
) -> None:
    await admin.create(topic, subscription)
    broker = PubSubBroker(
        project_id=settings.project_id, emulator_host=settings.emulator_host
    )

    async with broker:
        await broker.connect()
        await broker.publish(
            {"id": 1},
            topic,
            headers={"kind": "order"},
            ordering_key="k1",
            correlation_id="c-1",
        )

    response = await admin.subscriber.pull(
        subscription=admin.subscription_path(subscription), max_messages=10
    )
    [received] = response.received_messages
    assert received.message.data == b'{"id":1}'
    assert received.message.ordering_key == "k1"
    assert dict(received.message.attributes) == {
        "kind": "order",
        "correlation_id": "c-1",
        "content-type": "application/json",
    }


async def test_publish_to_missing_topic_raises(settings: Settings, topic: str) -> None:
    broker = PubSubBroker(
        project_id=settings.project_id, emulator_host=settings.emulator_host
    )

    async with broker:
        await broker.connect()
        with pytest.raises(NotFound):
            await broker.publish("x", topic)


async def test_publish_batch_is_one_request(
    settings: Settings, admin: Admin, topic: str, subscription: str
) -> None:
    await admin.create(topic, subscription)
    broker = PubSubBroker(
        project_id=settings.project_id, emulator_host=settings.emulator_host
    )

    async with broker:
        await broker.connect()
        await broker.publish_batch("a", "b", "c", topic=topic)

    response = await admin.subscriber.pull(
        subscription=admin.subscription_path(subscription), max_messages=10
    )
    assert sorted(m.message.data for m in response.received_messages) == [
        b"a",
        b"b",
        b"c",
    ]


async def test_publisher_batch_carries_the_publisher_settings(
    settings: Settings, admin: Admin, topic: str, subscription: str
) -> None:
    await admin.create(topic, subscription, enable_message_ordering=True)
    broker = PubSubBroker(
        project_id=settings.project_id, emulator_host=settings.emulator_host
    )
    publisher = broker.publisher(topic, headers={"kind": "order"}, ordering_key="k1")

    async with broker:
        await broker.connect()
        ids = await publisher.publish_batch("a", "b", headers={"extra": "1"})

    response = await admin.subscriber.pull(
        subscription=admin.subscription_path(subscription), max_messages=10
    )
    received = sorted(response.received_messages, key=lambda m: m.message.data)
    assert sorted(m.message.message_id for m in received) == sorted(ids)
    assert [m.message.data for m in received] == [b"a", b"b"]
    for m in received:
        assert m.message.ordering_key == "k1"
        assert m.message.attributes["kind"] == "order"
        assert m.message.attributes["extra"] == "1"
