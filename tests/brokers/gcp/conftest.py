import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import uuid4

import pytest
import pytest_asyncio
from google.pubsub_v1 import PublisherAsyncClient, SubscriberAsyncClient

PROJECT = "faststream-test"


@dataclass(frozen=True)
class Settings:
    project_id: str = PROJECT
    emulator_host: str = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def topic() -> str:
    return f"t-{uuid4().hex}"


@pytest.fixture()
def subscription() -> str:
    return f"s-{uuid4().hex}"


@dataclass
class Admin:
    """Direct GAPIC access to the emulator, so a live test checks the wire, not the broker."""

    publisher: PublisherAsyncClient
    subscriber: SubscriberAsyncClient
    project_id: str

    def topic_path(self, name: str) -> str:
        return f"projects/{self.project_id}/topics/{name}"

    def subscription_path(self, name: str) -> str:
        return f"projects/{self.project_id}/subscriptions/{name}"

    async def create(
        self, topic: str, subscription: str | None = None, **kwargs: object
    ) -> None:
        await self.publisher.create_topic(name=self.topic_path(topic))
        if subscription is not None:
            await self.subscriber.create_subscription(
                request={
                    "name": self.subscription_path(subscription),
                    "topic": self.topic_path(topic),
                    **kwargs,
                },
            )


@pytest_asyncio.fixture()
async def admin(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[Admin]:
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", settings.emulator_host)
    publisher = PublisherAsyncClient()
    subscriber = SubscriberAsyncClient()
    try:
        yield Admin(publisher, subscriber, settings.project_id)
    finally:
        await publisher.transport.close()
        await subscriber.transport.close()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # A shared testcase marked `connected` needs a real service: here, the emulator.
    for item in items:
        if item.get_closest_marker("connected") and not item.get_closest_marker("gcp"):
            item.add_marker(pytest.mark.gcp)
