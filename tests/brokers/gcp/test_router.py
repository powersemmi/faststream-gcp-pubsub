import pytest

from faststream_gcp_pubsub import PubSubBroker, PubSubPublisher, PubSubRoute
from tests.brokers.base.include_router import (
    IncludePublisherTestcase,
    IncludeSubscriberTestcase,
)
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import PubSubMemoryTestcaseConfig, PubSubTestcaseConfig


@pytest.mark.connected()
@pytest.mark.gcp()
@pytest.mark.asyncio()
class TestRouter(PubSubTestcaseConfig, RouterTestcase):
    route_class = PubSubRoute
    publisher_class = PubSubPublisher


@pytest.mark.asyncio()
class TestRouterLocal(PubSubMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = PubSubRoute
    publisher_class = PubSubPublisher


class TestIncludeSubscriber(PubSubMemoryTestcaseConfig, IncludeSubscriberTestcase):
    pass


class TestIncludePublisher(PubSubMemoryTestcaseConfig, IncludePublisherTestcase):
    pass


def test_include_router_refuses_a_foreign_router() -> None:
    broker = PubSubBroker(project_id="p")

    with pytest.raises(TypeError, match="PubSubRegistrator, got object"):
        broker.include_router(object())  # type: ignore[arg-type]
