import pytest

from faststream_gcp_pubsub import PubSubBroker, PubSubRouter
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


def test_a_subscriber_channel_is_addressed_by_its_topic() -> None:
    broker = PubSubBroker()

    @broker.subscriber("orders-sub", topic="orders")
    async def handle(body: str) -> None: ...

    ((name, channel),) = get_3_0_0_schema(broker)["channels"].items()

    # the key names the subscription that is read, the address the topic it is attached to
    assert name == "orders-sub:Handle"
    assert channel["address"] == "orders"


def test_a_subscription_without_a_topic_is_addressed_by_its_own_name() -> None:
    broker = PubSubBroker()

    @broker.subscriber("orders")
    async def handle(body: str) -> None: ...

    ((_, channel),) = get_3_0_0_schema(broker)["channels"].items()

    assert channel["address"] == "orders"


def test_a_publisher_channel_is_addressed_by_its_topic() -> None:
    broker = PubSubBroker()
    broker.publisher("orders")

    ((name, channel),) = get_3_0_0_schema(broker)["channels"].items()

    assert name == "orders:Publisher"
    assert channel["address"] == "orders"


def test_a_router_prefix_applies_to_the_subscription_and_the_topic() -> None:
    broker = PubSubBroker()
    router = PubSubRouter(prefix="app-")

    @router.subscriber("orders-sub", topic="orders")
    async def handle(body: str) -> None: ...

    router.publisher("events")
    broker.include_router(router)

    channels = get_3_0_0_schema(broker)["channels"]

    assert {name: channel["address"] for name, channel in channels.items()} == {
        "app-orders-sub:Handle": "app-orders",
        "app-events:Publisher": "app-events",
    }
