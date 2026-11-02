import pytest

from docs.docs_src.topics.declarations import bill, broker
from faststream_gcp_pubsub.testing import TestPubSubBroker


@pytest.mark.asyncio()
async def test_declared_subscription_reads_its_topic() -> None:
    async with TestPubSubBroker(broker) as br:
        await br.publish({"id": 1}, "orders")

        bill.mock.assert_called_once_with({"id": 1})
