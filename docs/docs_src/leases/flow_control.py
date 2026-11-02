from faststream import FastStream

from faststream_gcp_pubsub import FlowControl, PubSubBroker

# --8<-- [start:broker]
broker = PubSubBroker(project_id="my-project", graceful_timeout=30.0)
# --8<-- [end:broker]
app = FastStream(broker)


@broker.subscriber(
    "orders-billing",
    topic="orders",
    flow_control=FlowControl(
        max_messages=100,
        max_bytes=10 * 1024 * 1024,
        ack_deadline=60,
        max_lease=1800.0,
        ack_batch_wait=0.05,
    ),
    max_workers=10,
)
async def bill(order: dict[str, int]) -> None: ...
