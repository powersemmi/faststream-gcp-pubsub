from typing import Any

import pytest
from faststream.exceptions import IncorrectState

from faststream_gcp_pubsub import PubSubBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .conftest import Settings


def test_constructor_does_no_io() -> None:
    broker = PubSubBroker(project_id="p", emulator_host="localhost:1")

    assert broker._connection is None


@pytest.mark.gcp()
@pytest.mark.asyncio()
async def test_connect_and_ping(settings: Settings) -> None:
    broker = PubSubBroker(
        project_id=settings.project_id, emulator_host=settings.emulator_host
    )

    async with broker:
        await broker.connect()
        assert await broker.ping(timeout=5)

    assert not await broker.ping(timeout=1)


@pytest.mark.gcp()
@pytest.mark.asyncio()
async def test_ping_fails_without_server() -> None:
    broker = PubSubBroker(project_id="p", emulator_host="localhost:1")

    async with broker:
        await broker.connect()
        assert not await broker.ping(timeout=0.5)


@pytest.mark.asyncio()
async def test_publish_after_stop_raises() -> None:
    broker = PubSubBroker(project_id="p", emulator_host="localhost:1")
    publisher = broker.publisher("orders")

    await broker.connect()
    await broker.stop()

    with pytest.raises(IncorrectState):
        await broker.publish("x", "orders")
    with pytest.raises(IncorrectState):
        await publisher.publish("x")


@pytest.mark.asyncio()
async def test_declare_before_connect_raises() -> None:
    broker = PubSubBroker(project_id="p", emulator_host="localhost:1")

    with pytest.raises(IncorrectState, match="not connected"):
        await broker.declare_topic("orders")


@pytest.mark.connected()
@pytest.mark.gcp()
class TestConnection(BrokerConnectionTestcase):
    broker = PubSubBroker

    def get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {
            "project_id": settings.project_id,
            "emulator_host": settings.emulator_host,
        }
