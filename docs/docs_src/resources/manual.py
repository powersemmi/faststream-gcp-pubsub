from faststream import FastStream

from faststream_gcp_pubsub import PubSub, PubSubBroker, PubSubTopic

broker = PubSubBroker(project_id="my-project")
app = FastStream(broker)


@app.after_startup
async def declare() -> None:
    topic = await broker.declare_topic(PubSubTopic("orders", labels={"team": "billing"}))
    subscription = await broker.declare_subscription(
        PubSub("orders-billing", ack_deadline_seconds=30),
        topic="orders",
    )
    # both return the full resource name
    assert topic == "projects/my-project/topics/orders"
    assert subscription == "projects/my-project/subscriptions/orders-billing"
