from datetime import timedelta

from faststream import FastStream

from faststream_gcp_pubsub import (
    DeadLetterPolicy,
    PubSub,
    PubSubBroker,
    PubSubTopic,
    RetryPolicy,
)

broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)

orders = PubSubTopic(
    "orders",
    declare=True,
    labels={"team": "billing"},
    message_retention_duration=timedelta(days=1),
)

billing = PubSub(
    "orders-billing",
    declare=True,
    ack_deadline_seconds=30,
    enable_message_ordering=True,
    enable_exactly_once_delivery=True,
    filter='attributes.region = "eu"',
    dead_letter_policy=DeadLetterPolicy("orders-dead", max_delivery_attempts=5),
    retry_policy=RetryPolicy(
        minimum_backoff=timedelta(seconds=10),
        maximum_backoff=timedelta(seconds=300),
    ),
    message_retention_duration=timedelta(days=2),
    labels={"team": "billing"},
)


@broker.subscriber(billing, topic=orders)
async def bill(order: dict[str, int]) -> None: ...
