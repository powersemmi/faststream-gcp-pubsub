from faststream import FastStream

from faststream_gcp_pubsub import PubSubBroker

broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)


@broker.subscriber("orders-billing", topic="orders")
async def bill(order: dict[str, int]) -> None: ...


orders = broker.publisher(
    "orders",
    ordering_key="customer-42",
    headers={"source": "api"},
)


# a publisher object keeps its defaults for every call
async def place_order() -> None:
    await orders.publish({"id": 1})
    await orders.publish({"id": 2}, ordering_key="customer-43")
