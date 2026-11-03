from faststream import AckPolicy, FastStream, Logger

from faststream_gcp_pubsub import PubSub, PubSubBroker, PubSubMessage

broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)

billing = PubSub(
    "orders-billing",
    declare=True,
    enable_exactly_once_delivery=True,
)


@broker.subscriber(billing, topic="orders", ack_policy=AckPolicy.MANUAL)
async def bill(order: dict[str, int], msg: PubSubMessage, logger: Logger) -> None:
    try:
        # waits for the service to confirm the acknowledgement
        await msg.ack()
    except Exception:
        logger.exception("order %s was not confirmed", order["id"])
        raise
