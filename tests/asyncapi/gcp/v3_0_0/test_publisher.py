import pytest

from faststream_gcp_pubsub import PubSubBroker
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


class TestPublisher(PublisherTestcase):
    broker_class = PubSubBroker
