import pytest

from faststream_gcp_pubsub import PubSubBroker
from tests.asyncapi.base.v3_0_0.arguments import ArgumentsTestcase
from tests.asyncapi.gcp.described_channel import DescribedChannelMixin

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


class TestArguments(DescribedChannelMixin, ArgumentsTestcase):
    broker_class = PubSubBroker
