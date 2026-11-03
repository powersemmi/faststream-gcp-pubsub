"""Declarations of Pub/Sub resources: topics and subscriptions."""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from google.pubsub_v1 import Subscription, Topic

__all__ = (
    "DeadLetterPolicy",
    "PubSub",
    "PubSubTopic",
    "RetryPolicy",
    "resource_path",
)


def resource_path(project_id: str, kind: str, name: str) -> str:
    """The full resource name of a topic or subscription.

    A name that already starts with ``projects/`` is a full name and is kept as it is, so a
    subscriber can read a subscription of another project.

    Args:
        project_id: the project a short name belongs to.
        kind: ``topics`` or ``subscriptions``.
        name: a short id or a full resource name.
    """
    if name.startswith("projects/"):
        return name
    return f"projects/{project_id}/{kind}/{name}"


def short_name(name: str) -> str:
    """The id part of a resource name (``orders`` for ``projects/p/topics/orders``)."""
    return name.rsplit("/", 1)[-1]


@dataclass(frozen=True, slots=True)
class DeadLetterPolicy:
    """Where Pub/Sub moves a message after too many delivery attempts.

    Attributes:
        topic: the dead-letter topic, a short id or a full name.
        max_delivery_attempts: attempts before the move, 5 to 100.
    """

    topic: str
    max_delivery_attempts: int = 5


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """The backoff Pub/Sub applies between redeliveries of a nacked message.

    Attributes:
        minimum_backoff: the shortest delay, 0 to 600 seconds.
        maximum_backoff: the longest delay, 0 to 600 seconds.
    """

    minimum_backoff: timedelta = timedelta(seconds=10)
    maximum_backoff: timedelta = timedelta(seconds=600)


@dataclass(frozen=True, slots=True)
class PubSubTopic:
    """A Pub/Sub topic.

    Attributes:
        name: a short id (expanded with the broker's project) or a full resource name.
        declare: create the topic at startup when it does not exist.
        labels: labels of a created topic.
        message_retention_duration: how long a created topic keeps messages for seek.
    """

    name: str
    declare: bool = False
    labels: dict[str, str] = field(default_factory=dict)
    message_retention_duration: timedelta | None = None

    def path(self, project_id: str) -> str:
        """The full resource name in the given project."""
        return resource_path(project_id, "topics", self.name)

    def to_proto(self, project_id: str) -> "Topic":
        """The topic resource the broker creates."""
        from google.pubsub_v1 import Topic  # ruff: ignore[import-outside-top-level]

        options: dict[str, Any] = {"name": self.path(project_id), "labels": self.labels}
        if self.message_retention_duration is not None:
            options["message_retention_duration"] = self.message_retention_duration
        return Topic(**options)


@dataclass(frozen=True, slots=True)
class PubSub:
    """A Pub/Sub subscription and the settings a created one gets.

    The settings apply when the broker creates the subscription (``declare=True``); an existing
    subscription keeps its own.

    Attributes:
        name: a short id (expanded with the broker's project) or a full resource name.
        declare: create the subscription (and its topic) at startup when it does not exist.
        ack_deadline_seconds: the server-side ack deadline of a created subscription, 10 to 600.
        enable_message_ordering: deliver messages of one ordering key in publish order.
        enable_exactly_once_delivery: let an acknowledged message never be redelivered.
        filter: a filter expression on attributes; messages it rejects are acknowledged by the
            service and never delivered.
        dead_letter_policy: where messages go after too many delivery attempts.
        retry_policy: the backoff between redeliveries.
        message_retention_duration: how long unacknowledged messages are kept.
        labels: labels of a created subscription.
    """

    name: str
    declare: bool = False
    ack_deadline_seconds: int | None = None
    enable_message_ordering: bool = False
    enable_exactly_once_delivery: bool = False
    filter: str = ""
    dead_letter_policy: DeadLetterPolicy | None = None
    retry_policy: RetryPolicy | None = None
    message_retention_duration: timedelta | None = None
    labels: dict[str, str] = field(default_factory=dict)

    def path(self, project_id: str) -> str:
        """The full resource name in the given project."""
        return resource_path(project_id, "subscriptions", self.name)

    def to_proto(self, project_id: str, topic: str) -> "Subscription":
        """The subscription resource the broker creates, attached to ``topic`` (a full name)."""
        from google.pubsub_v1 import Subscription  # ruff: ignore[import-outside-top-level]

        options: dict[str, Any] = {
            "name": self.path(project_id),
            "topic": topic,
            "enable_message_ordering": self.enable_message_ordering,
            "enable_exactly_once_delivery": self.enable_exactly_once_delivery,
            "filter": self.filter,
            "labels": self.labels,
        }
        if self.ack_deadline_seconds is not None:
            options["ack_deadline_seconds"] = self.ack_deadline_seconds
        if self.message_retention_duration is not None:
            options["message_retention_duration"] = self.message_retention_duration
        if self.dead_letter_policy is not None:
            options["dead_letter_policy"] = {
                "dead_letter_topic": resource_path(
                    project_id, "topics", self.dead_letter_policy.topic
                ),
                "max_delivery_attempts": self.dead_letter_policy.max_delivery_attempts,
            }
        if self.retry_policy is not None:
            options["retry_policy"] = {
                "minimum_backoff": self.retry_policy.minimum_backoff,
                "maximum_backoff": self.retry_policy.maximum_backoff,
            }
        return Subscription(**options)
