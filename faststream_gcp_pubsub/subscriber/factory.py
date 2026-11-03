from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber.call_item import CallsCollection

from faststream_gcp_pubsub.schemas import PubSub, PubSubTopic

from .config import PubSubSubscriberConfig, PubSubSubscriberSpecificationConfig
from .settler import FlowControl
from .specification import PubSubSubscriberSpecification
from .usecase import PubSubConcurrentSubscriber, PubSubDefaultSubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy

    from faststream_gcp_pubsub.broker.config import PubSubBrokerConfig

SubscriberType = PubSubDefaultSubscriber | PubSubConcurrentSubscriber


def create_subscriber(
    *,
    subscription: str | PubSub,
    topic: str | PubSubTopic | None,
    flow_control: FlowControl | None,
    # Subscriber args
    ack_policy: "AckPolicy",
    no_reply: bool,
    config: "PubSubBrokerConfig",
    max_workers: int = 1,
    # AsyncAPI args
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> SubscriberType:
    subscription_ = (
        subscription if isinstance(subscription, PubSub) else PubSub(subscription)
    )
    if topic is None:
        # A subscription named alone is taken as attached to the topic of the same name.
        topic_ = PubSubTopic(subscription_.name)
    else:
        topic_ = topic if isinstance(topic, PubSubTopic) else PubSubTopic(topic)

    flow = flow_control or FlowControl()
    if max_workers > flow.max_messages:
        msg = f"max_workers={max_workers} exceeds flow_control.max_messages={flow.max_messages}"
        raise ValueError(msg)

    subscriber_config = PubSubSubscriberConfig(
        subscription=subscription_,
        topic=topic_,
        flow_control=flow,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    specification_config = PubSubSubscriberSpecificationConfig(
        subscription=subscription_,
        topic=topic_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    calls = CallsCollection[Any]()
    specification = PubSubSubscriberSpecification(config, specification_config, calls)

    if max_workers > 1:
        return PubSubConcurrentSubscriber(
            subscriber_config,
            specification,
            calls,
            max_workers=max_workers,
        )

    return PubSubDefaultSubscriber(subscriber_config, specification, calls)
