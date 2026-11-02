import asyncio
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
import pytest_asyncio
from faststream.exceptions import FeatureNotSupportedException
from faststream.opentelemetry.consts import (
    MESSAGING_DESTINATION_PUBLISH_NAME,
    MessageAction as Action,
)
from opentelemetry.sdk.trace import Span, TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_GCP_PUBSUB_MESSAGE_ORDERING_KEY,
)
from opentelemetry.semconv.trace import SpanAttributes

from faststream_gcp_pubsub import PubSubBroker
from faststream_gcp_pubsub.opentelemetry import PubSubTelemetryMiddleware
from faststream_gcp_pubsub.testing import TestPubSubBroker
from tests.brokers.gcp.basic import PubSubInProcessTestcaseConfig, PubSubTestcaseConfig
from tests.brokers.gcp.conftest import Admin
from tests.brokers.gcp.test_consume import TestConsume as ConsumeCase
from tests.brokers.gcp.test_publish import TestPublish as PublishCase
from tests.opentelemetry.basic import LocalTelemetryTestcase


class MaskedSpan:
    """A span whose numeric message id reads as the UUID the shared check expects."""

    def __init__(self, span: Span) -> None:
        self._span = span
        self.attributes = dict(span.attributes or {})
        if SpanAttributes.MESSAGING_MESSAGE_ID in self.attributes:
            self.attributes[SpanAttributes.MESSAGING_MESSAGE_ID] = str(uuid4())

    def __getattr__(self, name: str) -> Any:
        return getattr(self._span, name)


class TelemetryCases(LocalTelemetryTestcase):
    messaging_system = "gcp_pubsub"
    include_messages_counters = True
    telemetry_middleware_class = PubSubTelemetryMiddleware

    def assert_span(
        self,
        span: Span,
        action: str,
        queue: str,
        msg: str,
        parent_span_id: int | None = None,
    ) -> None:
        # The service numbers messages ("1234567890"), where the shared check expects a UUID.
        attrs = span.attributes or {}
        if SpanAttributes.MESSAGING_MESSAGE_ID in attrs:
            assert str(attrs[SpanAttributes.MESSAGING_MESSAGE_ID]).isdigit()
        masked = MaskedSpan(span)
        super().assert_span(masked, action, queue, msg, parent_span_id)  # type: ignore[arg-type]

    async def test_ordering_key_and_batch_attributes(
        self,
        queue: str,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
        event: asyncio.Event,
    ) -> None:
        mid = self.telemetry_middleware_class(tracer_provider=tracer_provider)
        broker: PubSubBroker = self.get_broker(middlewares=(mid,))
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish_batch("a", "b", topic=queue, ordering_key="key")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        by_name = {s.name: s for s in self.get_spans(trace_exporter)}
        publish = by_name[f"{queue} publish"]
        process = by_name[f"{queue} process"]
        assert (publish.attributes or {})[
            SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT
        ] == 2
        assert (publish.attributes or {})[
            MESSAGING_GCP_PUBSUB_MESSAGE_ORDERING_KEY
        ] == "key"
        process_attrs = process.attributes or {}
        assert process_attrs[MESSAGING_GCP_PUBSUB_MESSAGE_ORDERING_KEY] == "key"
        assert process_attrs[MESSAGING_DESTINATION_PUBLISH_NAME] == queue

    async def test_no_ordering_key_attribute_without_key(
        self,
        queue: str,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        mid = self.telemetry_middleware_class(tracer_provider=tracer_provider)
        broker: PubSubBroker = self.get_broker(middlewares=(mid,))
        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("a", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        for span in self.get_spans(trace_exporter):
            assert MESSAGING_GCP_PUBSUB_MESSAGE_ORDERING_KEY not in (
                span.attributes or {}
            )

    async def test_request_is_not_supported(self, queue: str) -> None:
        # Pub/Sub has no request/reply, so there is no request span to observe.
        broker: PubSubBroker = self.get_broker(
            middlewares=(self.telemetry_middleware_class(),)
        )
        async with self.patch_broker(broker) as br:
            await br.start()
            with pytest.raises(FeatureNotSupportedException):
                await br.request("", queue)


@pytest.mark.asyncio()
class TestTelemetry(PubSubInProcessTestcaseConfig, TelemetryCases):
    async def test_no_trace_context_create_process_span(
        self,
        queue: str,
        tracer_provider: TracerProvider,
        trace_exporter: InMemorySpanExporter,
        event: asyncio.Event,
    ) -> None:
        # The shared case enters two brokers one by one; each in-process test broker has its
        # own service, so the plain broker's publish would never reach the traced one. Both
        # join one test broker here.
        traced = self.get_broker(
            middlewares=(
                self.telemetry_middleware_class(tracer_provider=tracer_provider),
            )
        )
        plain = self.get_broker()
        args, kwargs = self.get_subscriber_params(queue)

        @traced.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(m: Any) -> None:
            event.set()

        async with TestPubSubBroker(traced._broker, plain._broker):
            await plain.publish("start", queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        create, process = self.get_spans(trace_exporter)
        parent_span_id = create.context.span_id
        self.assert_span(create, Action.CREATE, queue, "start")
        self.assert_span(process, Action.PROCESS, queue, "start", parent_span_id)


@pytest.mark.connected()
@pytest.mark.gcp()
@pytest.mark.asyncio()
@pytest.mark.usefixtures("reply_topics")
class TestTelemetryLive(PubSubTestcaseConfig, TelemetryCases):
    @pytest_asyncio.fixture()
    async def reply_topics(self, admin: Admin, queue: str) -> None:
        # The shared finalize-tokens case publishes the handler result to these topics, and
        # the service refuses a publish to a topic nobody created.
        for suffix in ("out1", "out2"):
            await admin.publisher.create_topic(name=admin.topic_path(f"{queue}-{suffix}"))


@pytest.mark.connected()
@pytest.mark.gcp()
class TestPublishWithTelemetry(PublishCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> PubSubBroker:
        return super().get_broker(
            apply_types=apply_types,
            middlewares=(PubSubTelemetryMiddleware(),),
            **kwargs,
        )


@pytest.mark.connected()
@pytest.mark.gcp()
class TestConsumeWithTelemetry(ConsumeCase):
    def get_broker(self, apply_types: bool = False, **kwargs: Any) -> PubSubBroker:
        return super().get_broker(
            apply_types=apply_types,
            middlewares=(PubSubTelemetryMiddleware(),),
            **kwargs,
        )
