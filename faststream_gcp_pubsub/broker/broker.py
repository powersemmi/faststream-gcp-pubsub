import contextlib
import logging
import os
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from fast_depends import Provider, dependency_provider
from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream._internal.types import IdGenerator
from faststream.exceptions import IncorrectState
from faststream.message import gen_cor_id
from faststream.middlewares import AckPolicy
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec
from google.api_core.exceptions import AlreadyExists
from typing_extensions import override

from faststream_gcp_pubsub.broker.config import PubSubBrokerConfig
from faststream_gcp_pubsub.connection import ConnectionOptions, PubSubConnection
from faststream_gcp_pubsub.message import PubSubRawMessage
from faststream_gcp_pubsub.publisher.producer import PubSubProducer
from faststream_gcp_pubsub.response import PubSubPublishCommand
from faststream_gcp_pubsub.schemas import PubSub, PubSubTopic, resource_path
from faststream_gcp_pubsub.security import parse_security
from faststream_gcp_pubsub.subscriber.usecase import PubSubBaseSubscriber

from .logging import make_pubsub_logger_state
from .registrator import PubSubRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

__all__ = ("PubSubBroker",)

DEFAULT_ENDPOINT = "pubsub.googleapis.com:443"


class PubSubBroker(
    PubSubRegistrator,
    BrokerUsecase[PubSubRawMessage, PubSubConnection, PubSubBrokerConfig],
):
    """Google Cloud Pub/Sub broker.

    The constructor only records the configuration; ``connect()`` (or ``start()``) builds the
    clients.

    Args:
        project_id: the project short topic and subscription names belong to; read from
            ``GOOGLE_CLOUD_PROJECT`` when omitted.
        emulator_host: ``host:port`` of a Pub/Sub emulator; read from
            ``PUBSUB_EMULATOR_HOST`` when omitted. ``None`` connects to Google Cloud even when
            the variable is set.
        api_endpoint: a regional or private endpoint instead of the global one.
        client_options: other ``google.api_core.client_options`` for both clients.
        security: a ``GCPSecurity`` with the credentials to use; Application Default
            Credentials when omitted.
    """

    def __init__(
        self,
        project_id: str = EMPTY,
        *,
        emulator_host: str | None = EMPTY,
        api_endpoint: str | None = None,
        client_options: dict[str, Any] | None = None,
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        dependencies: Sequence["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[PubSubRegistrator] = (),
        ack_policy: AckPolicy = EMPTY,
        id_generator: IdGenerator = gen_cor_id,
        # AsyncAPI args
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "v1",
        description: str | None = None,
        tags: Iterable["Tag | TagDict"] = (),
        security: Optional["BaseSecurity"] = None,
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        if project_id is EMPTY:
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
        if emulator_host is EMPTY:
            emulator_host = os.environ.get("PUBSUB_EMULATOR_HOST") or None

        self._connection_options = ConnectionOptions(
            emulator_host=emulator_host,
            credentials=parse_security(security),
            api_endpoint=api_endpoint,
            client_options=client_options,
        )

        if specification_url is None:
            specification_url = emulator_host or api_endpoint or DEFAULT_ENDPOINT

        super().__init__(
            # broker config
            routers=routers,
            config=PubSubBrokerConfig(
                project_id=project_id,
                producer=PubSubProducer(parser=parser, decoder=decoder),
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_pubsub_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                id_generator=id_generator,
                extra_context={
                    "broker": self,
                },
            ),
            specification=BrokerSpec(
                description=description,
                url=[specification_url],
                protocol=protocol or "googlepubsub",
                protocol_version=protocol_version,
                tags=tags,
                security=security,
            ),
        )

    @property
    def project_id(self) -> str:
        """The project short names belong to."""
        project_id: str = self.config.project_id
        return project_id

    @override
    async def _connect(self) -> PubSubConnection:
        if not self.config.project_id:
            msg = "PubSubBroker needs a project_id (or GOOGLE_CLOUD_PROJECT) to connect."
            raise IncorrectState(msg)
        connection = PubSubConnection.open(self._connection_options)
        self.config.connect(connection)
        return connection

    @override
    async def start(self) -> None:
        await self.connect()
        c = PubSubBaseSubscriber.build_log_context(None, "")
        self.config.logger.log("Connection established", logging.INFO, c)
        await super().start()

    @override
    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)

        if self._connection is not None:
            await self._connection.close()
            self._connection = None

        self.config.disconnect()

    @override
    async def ping(self, timeout: float | None = None) -> bool:
        if self._connection is None:
            return False
        connection: PubSubConnection = self._connection
        return await connection.ready(timeout or 5.0)

    async def declare_topic(self, topic: str | PubSubTopic) -> str:
        """Create a topic unless it exists.

        Returns:
            The full resource name of the topic.
        """
        topic_ = topic if isinstance(topic, PubSubTopic) else PubSubTopic(topic)
        request = topic_.to_proto(self.config.project_id)
        with contextlib.suppress(AlreadyExists):
            await self.config.connection.publisher.create_topic(request=request)
        return str(request.name)

    async def declare_subscription(
        self,
        subscription: str | PubSub,
        topic: str | PubSubTopic,
    ) -> str:
        """Create a subscription attached to ``topic`` unless it exists; the topic must exist.

        Returns:
            The full resource name of the subscription.
        """
        subscription_ = (
            subscription if isinstance(subscription, PubSub) else PubSub(subscription)
        )
        topic_name = topic.name if isinstance(topic, PubSubTopic) else topic
        project = self.config.project_id
        request = subscription_.to_proto(
            project, resource_path(project, "topics", topic_name)
        )
        with contextlib.suppress(AlreadyExists):
            await self.config.connection.subscriber.create_subscription(request=request)
        return str(request.name)

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        topic: str = "",
        *,
        headers: dict[str, str] | None = None,
        ordering_key: str = "",
        correlation_id: str | None = None,
        reply_to: str = "",
        timeout: float | None = None,
    ) -> str:
        """Publish a message to a topic.

        Args:
            message: the message body.
            topic: the topic, a short id or a full resource name.
            headers: message attributes.
            ordering_key: messages of one key are delivered in publish order to a subscription
                with message ordering; publish them one after another.
            correlation_id: the correlation id; generated when omitted.
            reply_to: the topic a consumer publishes its reply to.
            timeout: the RPC deadline in seconds; the client's default when omitted.

        Returns:
            The message id the service assigned.
        """
        cmd = PubSubPublishCommand(
            message,
            topic=topic,
            headers=headers,
            ordering_key=ordering_key,
            correlation_id=correlation_id or self.config.id_generator(),
            reply_to=reply_to,
            timeout=timeout,
            _publish_type=PublishType.PUBLISH,
        )
        result: str = await self._basic_publish(cmd, producer=self.config.producer)
        return result

    async def publish_batch(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        topic: str,
        headers: dict[str, str] | None = None,
        ordering_key: str = "",
        correlation_id: str | None = None,
        reply_to: str = "",
        timeout: float | None = None,
    ) -> list[str]:
        """Publish several messages to a topic in one request.

        The service takes at most 1000 messages and 10 MB in one request.

        Returns:
            The message ids, in the order of the messages.
        """
        cmd = PubSubPublishCommand(
            *messages,
            topic=topic,
            headers=headers,
            ordering_key=ordering_key,
            correlation_id=correlation_id or self.config.id_generator(),
            reply_to=reply_to,
            timeout=timeout,
            _publish_type=PublishType.PUBLISH,
        )
        result: list[str] = await self._basic_publish_batch(
            cmd, producer=self.config.producer
        )
        return result

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage" = None,
        topic: str = "",
        *,
        correlation_id: str | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Not available on Pub/Sub; raises ``FeatureNotSupportedException``.

        Publish with ``reply_to`` naming a topic your service subscribes to instead.
        """
        cmd = PubSubPublishCommand(
            message,
            topic=topic,
            correlation_id=correlation_id or self.config.id_generator(),
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
        )
        return await self._basic_request(cmd, producer=self.config.producer)
