from faststream import FastStream

from faststream_gcp_pubsub import PubSubBroker, PubSubSubscription, PubSubTopic

broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)


@broker.subscriber(
    PubSubSubscription("orders-billing", declare=True),
    topic=PubSubTopic("orders"),
)
async def bill(order: dict[str, int]) -> None: ...
