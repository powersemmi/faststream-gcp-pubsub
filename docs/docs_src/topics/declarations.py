from faststream import FastStream

from faststream_gcp_pubsub import PubSub, PubSubBroker, PubSubTopic

broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)


@broker.subscriber(
    PubSub("orders-billing", declare=True),
    topic=PubSubTopic("orders"),
)
async def bill(order: dict[str, int]) -> None: ...
