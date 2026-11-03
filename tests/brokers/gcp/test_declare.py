import pytest

from faststream_gcp_pubsub import PubSubBroker

from .conftest import Admin, Settings

pytestmark = [pytest.mark.gcp, pytest.mark.asyncio]


async def test_declared_subscription_receives_the_topic(
    settings: Settings, admin: Admin, topic: str, subscription: str
) -> None:
    broker = PubSubBroker(
        project_id=settings.project_id, emulator_host=settings.emulator_host
    )

    async with broker:
        await broker.connect()
        topic_name = await broker.declare_topic(topic)
        name = await broker.declare_subscription(subscription, topic)
        again = await broker.declare_subscription(subscription, topic)
        await broker.publish("hi", topic)

    assert topic_name == admin.topic_path(topic)
    assert name == again == admin.subscription_path(subscription)
    response = await admin.subscriber.pull(subscription=name, max_messages=10)
    assert [m.message.data for m in response.received_messages] == [b"hi"]
