# mypy: disable-error-code="untyped-decorator"
# `router_class` is held as `Any` by the shared testcases, so their decorators are untyped here.
from typing import Annotated, Any
from unittest.mock import Mock

import pytest
from fastapi import Depends, FastAPI, Header
from fastapi.testclient import TestClient
from faststream import Response
from faststream.exceptions import FeatureNotSupportedException, IncorrectState

from faststream_gcp_pubsub import (
    PubSubBroker,
    PubSubPublisher,
    PubSubRoute,
    PubSubRouter as BrokerRouter,
)
from faststream_gcp_pubsub.fastapi import PubSubRouter as StreamRouter
from tests.brokers.base.fastapi import FastAPILocalTestcase, FastAPITestcase
from tests.brokers.base.option_surface import (
    assert_every_level_takes_the_options_the_broker_takes,
)

from .basic import SETTINGS, PubSubMemoryTestcaseConfig, PubSubTestcaseConfig


def test_fastapi_router_takes_the_options_the_broker_takes() -> None:
    assert_every_level_takes_the_options_the_broker_takes(
        PubSubBroker,
        StreamRouter,
        PubSubRoute,
        PubSubPublisher,
    )


@pytest.mark.asyncio()
async def test_router_without_project_refuses_to_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Like the broker: no project is guessed, so a resource never lands in a project the
    # service did not name.
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    router = StreamRouter(emulator_host="localhost:1")

    with pytest.raises(IncorrectState, match="project_id"):
        await router.broker.connect()


class LiveStreamRouter(StreamRouter):
    """The FastAPI router a live test builds: the project and emulator of the suite."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("project_id", SETTINGS.project_id)
        kwargs.setdefault("emulator_host", SETTINGS.emulator_host)
        super().__init__(*args, **kwargs)


@pytest.mark.connected()
@pytest.mark.gcp()
class TestRouter(PubSubTestcaseConfig, FastAPITestcase):
    router_class = LiveStreamRouter
    broker_router_class = BrokerRouter


# Pub/Sub has no request/reply, so the base cases that read a handler result through
# `broker.request` are restated over `publish` and the subscriber mock; the scenario each one
# covers stays the same.
class TestRouterLocal(PubSubMemoryTestcaseConfig, FastAPILocalTestcase):
    router_class = StreamRouter
    broker_router_class = BrokerRouter

    async def test_base(self, queue: str) -> None:
        router = self.router_class()

        app = FastAPI()
        app.include_router(router)

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> str:
            return "hi"

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert client.app_state["broker"] is br

                await br.publish("hi", queue)
                hello.mock.assert_called_once_with("hi")

    async def test_request(self, queue: str) -> None:
        router = self.router_class(setup_state=False)

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> Any:
            return Response("Hi!", headers={"x-header": "test"})

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert not client.app_state.get("broker")

                with pytest.raises(FeatureNotSupportedException):
                    await br.request("hi", queue, timeout=0.5)

    async def test_base_without_state(self, queue: str) -> None:
        router = self.router_class(setup_state=False)

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> str:
            return "hi"

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert not client.app_state.get("broker")

                await br.publish("hi", queue)
                hello.mock.assert_called_once_with("hi")

    async def test_headers(self, queue: str) -> None:
        router = self.router_class()
        seen: list[str] = []

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(w: Any = Header()) -> None:
            seen.append(w)

        async with self.patch_broker(router.broker) as br:
            await br.publish("", queue, headers={"w": "hi"})

        assert seen == ["hi"]

    async def test_depends(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep(a: Any) -> Any:
            mock.dep(a)
            return a

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any, w: Any = Depends(dep)) -> None:
            mock.handler(w)

        async with self.patch_broker(router.broker) as br:
            await br.publish({"a": "hi"}, queue)

        mock.dep.assert_called_once_with("hi")
        mock.handler.assert_called_once_with("hi")

    async def test_mixed_depends(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep(a: str) -> str:
            mock.dep(a)
            return a

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a: str, w: Annotated[str, Depends(dep)]) -> None:
            mock.handler(w)

        async with self.patch_broker(router.broker) as br:
            await br.publish({"a": "hi"}, queue)

        mock.dep.assert_called_once_with("hi")
        mock.handler.assert_called_once_with("hi")

    async def test_yield_depends(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep(a: Any) -> Any:
            mock.start()
            yield a
            mock.close()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any, w: Any = Depends(dep)) -> None:
            mock.inside(started=mock.start.call_count, closed=mock.close.call_count)

        async with self.patch_broker(router.broker) as br:
            await br.publish({"a": "hi"}, queue)

        # the dependency is open while the handler runs and closed after it
        mock.inside.assert_called_once_with(started=1, closed=0)
        mock.start.assert_called_once()
        mock.close.assert_called_once()

    async def test_router_depends(self, mock: Mock, queue: str) -> None:
        def mock_dep() -> None:
            mock()

        router = self.router_class(dependencies=(Depends(mock_dep, use_cache=False),))

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any) -> None: ...

        async with self.patch_broker(router.broker) as br:
            await br.publish("hi", queue)
            hello.mock.assert_called_once_with("hi")

        mock.assert_called_once()

    async def test_subscriber_depends(self, mock: Mock, queue: str) -> None:
        def mock_dep() -> None:
            mock()

        router = self.router_class()

        args, kwargs = self.get_subscriber_params(
            queue,
            dependencies=(Depends(mock_dep, use_cache=False),),
        )

        @router.subscriber(*args, **kwargs)
        async def hello(a: Any) -> None: ...

        async with self.patch_broker(router.broker) as br:
            await br.publish("hi", queue)
            hello.mock.assert_called_once_with("hi")

        mock.assert_called_once()

    async def test_include(self, queue: str) -> None:
        router = self.router_class()
        router2 = self.broker_router_class()

        app = FastAPI()

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello() -> None: ...

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @router2.subscriber(*args2, **kwargs2)
        async def hello_router2() -> None: ...

        router.include_router(router2)
        app.include_router(router)

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert client.app_state["broker"] is br

                await br.publish("hi", queue)
                await br.publish("hi", queue + "1")

                hello.mock.assert_called_once_with("hi")
                hello_router2.mock.assert_called_once_with("hi")

    async def test_dependency_overrides(self, mock: Mock, queue: str) -> None:
        router = self.router_class()

        def dep1() -> None:
            raise AssertionError

        def dep2() -> None:
            mock()

        app = FastAPI()
        app.dependency_overrides[dep1] = dep2

        args, kwargs = self.get_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)
        async def hello_router2(dep: None = Depends(dep1)) -> None: ...

        app.include_router(router)

        async with self.patch_broker(router.broker) as br:
            with TestClient(app) as client:
                assert client.app_state["broker"] is br

                await br.publish("hi", queue)
                hello_router2.mock.assert_called_once_with("hi")

        mock.assert_called_once()
