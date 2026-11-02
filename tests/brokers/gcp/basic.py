from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Any, overload

from typing_extensions import Self, override

from faststream_gcp_pubsub import PubSubBroker, PubSubRouter, PubSubSubscription
from tests.brokers.base.basic import BaseTestcaseConfig

from .conftest import Settings

SETTINGS = Settings()


class PubSubTestcaseConfig(BaseTestcaseConfig[PubSubBroker]):
    """Live: the broker talks to the emulator and declares what a test subscribes to."""

    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> PubSubBroker:
        kwargs.setdefault("project_id", SETTINGS.project_id)
        kwargs.setdefault("emulator_host", SETTINGS.emulator_host)
        return PubSubBroker(apply_types=apply_types, **kwargs)

    def get_subscriber_params(
        self, subscription: str, *args: Any, **kwargs: Any
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (PubSubSubscription(subscription, declare=True), *args), kwargs

    def get_router(self, **kwargs: Any) -> PubSubRouter:
        return PubSubRouter(**kwargs)


class PubSubMemoryTestcaseConfig(PubSubTestcaseConfig):
    """In-process: the same tests through ``TestPubSubBroker``."""

    @overload
    def patch_broker(
        self, brokers: PubSubBroker, **kwargs: Any
    ) -> AbstractAsyncContextManager[PubSubBroker]: ...

    @overload
    def patch_broker(
        self, *brokers: PubSubBroker, **kwargs: Any
    ) -> AbstractAsyncContextManager[tuple[PubSubBroker, ...]]: ...

    @override
    def patch_broker(self, *brokers: PubSubBroker, **kwargs: Any) -> Any:
        from faststream_gcp_pubsub.testing import TestPubSubBroker  # ruff: ignore[import-outside-top-level]

        return TestPubSubBroker(*brokers, **kwargs)


class InProcessBroker:
    """A broker already patched for tests, shaped like a live one.

    Shared cases written for a live broker enter it with ``async with`` and call ``start`` and
    ``publish`` on it; this wrapper enters ``TestPubSubBroker`` instead and forwards the rest
    to the broker it wraps.
    """

    def __init__(self, broker: PubSubBroker) -> None:
        from faststream_gcp_pubsub.testing import TestPubSubBroker  # ruff: ignore[import-outside-top-level]

        self._broker = broker
        self._test = TestPubSubBroker(broker)

    async def __aenter__(self) -> Self:
        await self._test.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._test.__aexit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._broker, name)


class PubSubInProcessTestcaseConfig(PubSubMemoryTestcaseConfig):
    """In-process, for shared cases that run a broker without patching it.

    Every broker is built already patched, so such a case never reaches a real service.
    """

    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> Any:
        return InProcessBroker(super().get_broker(apply_types, **kwargs))

    def patch_broker(self, *brokers: Any, **kwargs: Any) -> Any:
        (broker,) = brokers
        return broker
