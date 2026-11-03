from dataclasses import dataclass, field

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares.acknowledgement.config import AckPolicy

from faststream_gcp_pubsub.broker.config import PubSubBrokerConfig
from faststream_gcp_pubsub.schemas import PubSub, PubSubTopic

from .settler import FlowControl


@dataclass(kw_only=True)
class PubSubSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    subscription: PubSub
    topic: PubSubTopic


@dataclass(kw_only=True)
class PubSubSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "PubSubBrokerConfig" = field(default_factory=PubSubBrokerConfig)

    subscription: PubSub
    topic: PubSubTopic
    flow_control: FlowControl

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            if self._outer_config.ack_policy is not EMPTY:
                return self._outer_config.ack_policy
            # A nack feeds the retry and dead-letter policies; a reject is an ack on Pub/Sub.
            return AckPolicy.NACK_ON_ERROR
        return self._ack_policy
