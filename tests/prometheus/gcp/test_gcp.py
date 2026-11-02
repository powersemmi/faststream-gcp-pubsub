import asyncio
from typing import Any

import pytest
from faststream.exceptions import FeatureNotSupportedException, IgnoredException
from faststream.message import AckStatus
from faststream.prometheus.consts import PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP
from faststream.prometheus.types import ProcessingStatus
from prometheus_client import CollectorRegistry

from faststream_gcp_pubsub import PubSubBroker
from faststream_gcp_pubsub.prometheus import PubSubPrometheusMiddleware
from tests.brokers.gcp.test_consume import TestConsume as ConsumeCase
from tests.brokers.gcp.test_publish import TestPublish as PublishCase
from tests.prometheus.basic import (
    LocalMetricsSettingsProviderTestcase,
    LocalPrometheusTestcase,
)

from .basic import PubSubLivePrometheusSettings, PubSubPrometheusSettings


class RequestNotSupported:
    async def test_rpc_request(self, queue: str) -> None:
        # Pub/Sub has no request/reply: the metrics of a request cannot exist.
        broker = self.get_broker(  # type: ignore[attr-defined]
            middlewares=(self.get_middleware(registry=CollectorRegistry()),)  # type: ignore[attr-defined]
        )
        async with self.patch_broker(broker) as br:  # type: ignore[attr-defined]
            await br.start()
            with pytest.raises(FeatureNotSupportedException):
                await br.request("", queue)


@pytest.mark.asyncio()
class TestPrometheus(
    RequestNotSupported, PubSubPrometheusSettings, LocalPrometheusTestcase
):
    async def test_metrics(
        self,
        queue: str,
        status: AckStatus,
        exception_class: type[Exception] | None,
        event: asyncio.Event,
    ) -> None:
        if exception_class not in {Exception, IgnoredException}:
            await super().test_metrics(queue, status, exception_class, event)
            return

        # In process the handler's exception reaches the publishing call, as FastStream's test
        # client raises it there, so the publish metrics differ from a live run; the full set
        # is checked by TestPrometheusLive. Here: the exception surfaces and the processing
        # metric records the status the middleware maps it to.
        registry = CollectorRegistry()
        broker = self.get_broker(
            apply_types=True, middlewares=(self.get_middleware(registry=registry),)
        )

        async def handler(m: Any) -> None:
            raise exception_class

        broker.subscriber(*self.get_subscriber_params(queue)[0])(handler)

        async with broker:
            await broker.start()
            with pytest.raises(exception_class):
                await broker.publish("hello", queue)

        expected = (
            PROCESSING_STATUS_BY_HANDLER_EXCEPTION_MAP.get(exception_class)
            or ProcessingStatus.error
        )
        statuses = [
            sample.labels["status"]
            for metric in registry.collect()
            if metric.name == "faststream_received_processed_messages"
            for sample in metric.samples
            if sample.name.endswith("_total")
        ]
        assert statuses == [expected.value]

    # the same cases as the shared test, which an override does not inherit
    test_metrics.pytestmark = LocalPrometheusTestcase.test_metrics.pytestmark  # type: ignore[attr-defined]


class TestMetricsSettingsProvider(
    PubSubPrometheusSettings, LocalMetricsSettingsProviderTestcase
):
    pass


@pytest.mark.connected()
@pytest.mark.gcp()
@pytest.mark.asyncio()
class TestPrometheusLive(
    RequestNotSupported, PubSubLivePrometheusSettings, LocalPrometheusTestcase
):
    pass


@pytest.mark.connected()
@pytest.mark.gcp()
class TestPublishWithPrometheus(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> PubSubBroker:
        return super().get_broker(
            apply_types=apply_types,
            middlewares=(PubSubPrometheusMiddleware(registry=CollectorRegistry()),),
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.gcp()
class TestConsumeWithPrometheus(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> PubSubBroker:
        return super().get_broker(
            apply_types=apply_types,
            middlewares=(PubSubPrometheusMiddleware(registry=CollectorRegistry()),),
            **kwargs,
        )
