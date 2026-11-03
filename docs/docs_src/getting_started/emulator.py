from faststream import FastStream, Logger

from faststream_gcp_pubsub import PubSub, PubSubBroker

broker = PubSubBroker(project_id="faststream-test", emulator_host="localhost:8085")
app = FastStream(broker)


@broker.subscriber(PubSub("orders-billing", declare=True), topic="orders")
async def handle(order: dict[str, int], logger: Logger) -> None:
    logger.info("order %s", order["id"])


@app.after_startup
async def place_order() -> None:
    await broker.publish({"id": 1}, "orders")
