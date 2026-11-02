from typing import Any

import pytest
from faststream.specification import Specification

from faststream_gcp_pubsub.fastapi import PubSubRouter
from faststream_gcp_pubsub.testing import TestPubSubBroker
from tests.asyncapi.base.v3_0_0.arguments import FastAPICompatible
from tests.asyncapi.base.v3_0_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase
from tests.asyncapi.gcp.described_channel import DescribedChannelMixin

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


class TestRouterArguments(DescribedChannelMixin, FastAPITestCase, FastAPICompatible):
    broker_class = PubSubRouter
    router_class = PubSubRouter
    broker_wrapper = staticmethod(TestPubSubBroker)

    def get_spec(self, *routers: Any) -> Specification:
        spec: Specification = super().get_spec(*(router.broker for router in routers))
        return spec


class TestRouterPublisher(PublisherTestcase):
    broker_class = PubSubRouter

    def get_spec(self, *routers: Any) -> Specification:
        spec: Specification = super().get_spec(*(router.broker for router in routers))
        return spec
