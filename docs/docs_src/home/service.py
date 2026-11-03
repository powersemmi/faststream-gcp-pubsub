from faststream import FastStream
from pydantic import BaseModel

from faststream_gcp_pubsub import PubSub, PubSubBroker


class Order(BaseModel):
    id: int
    amount: float


broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)

shipments = broker.publisher("shipments")


@broker.subscriber(PubSub("orders-billing"), topic="orders")
@shipments
async def bill(order: Order) -> dict[str, int]:
    # the return value is published to the `shipments` topic
    return {"order_id": order.id}


@app.after_startup
async def place_order() -> None:
    await broker.publish(Order(id=1, amount=9.5), "orders")
