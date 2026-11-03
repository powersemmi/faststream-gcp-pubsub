from faststream import FastStream

from faststream_gcp_pubsub import PubSub, PubSubBroker, PubSubTopic

broker = PubSubBroker(project_id="faststream-test")
app = FastStream(broker)

shipments = broker.publisher(PubSubTopic("shipments", declare=True))


@broker.subscriber(PubSub("orders-billing", declare=True), topic="orders")
@shipments
async def bill(order: dict[str, int]) -> dict[str, int]:
    return {"order_id": order["id"]}


@broker.subscriber(PubSub("shipments-courier", declare=True), topic="shipments")
async def ship(shipment: dict[str, int]) -> None: ...
