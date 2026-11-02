import pytest

from faststream_gcp_pubsub import PubSubBroker, PubSubRouter
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


def test_channels_are_named_by_subscription_and_publisher_topic() -> None:
    broker = PubSubBroker()

    @broker.subscriber("orders-sub", topic="orders")
    async def handle(body: str) -> None: ...

    broker.publisher("events")

    # 2.6.0 channels have no address field and Pub/Sub has no binding: the key names the
    # subscription and the description names the topic it reads
    channels = get_2_6_0_schema(broker)["channels"]
    assert list(channels) == ["orders-sub:Handle", "events:Publisher"]
    assert channels["orders-sub:Handle"]["description"] == (
        "Reads subscription `orders-sub` of topic `orders`."
    )


def test_a_router_prefix_applies_to_the_subscription_and_the_topic() -> None:
    broker = PubSubBroker()
    router = PubSubRouter(prefix="app-")

    @router.subscriber("orders-sub", topic="orders")
    async def handle(body: str) -> None: ...

    router.publisher("events")
    broker.include_router(router)

    assert list(get_2_6_0_schema(broker)["channels"]) == [
        "app-orders-sub:Handle",
        "app-events:Publisher",
    ]
