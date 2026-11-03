from faststream import FastStream, Logger

from faststream_gcp_pubsub import (
    DeadLetterPolicy,
    PubSub,
    PubSubBroker,
    PubSubMessage,
)

broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)

billing = PubSub(
    "orders-billing",
    declare=True,
    dead_letter_policy=DeadLetterPolicy("orders-dead", max_delivery_attempts=5),
)


@broker.subscriber(billing, topic="orders")
async def bill(order: dict[str, int], msg: PubSubMessage, logger: Logger) -> None:
    # counts deliveries; it is 0 on a subscription without a dead-letter policy
    logger.info("attempt %d of order %s", msg.delivery_attempt, order["id"])
    if msg.delivery_attempt < 3:
        error = "retry later"
        raise ValueError(error)
