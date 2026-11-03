# Declaring resources

A resource is created when its declaration says `declare=True`. The broker creates the topic
and the subscription at startup if they do not exist.

```python title="service.py"
--8<-- "docs/docs_src/resources/declare.py"
```

An existing resource keeps its own settings: the declaration applies only to a resource the
broker creates. The dead-letter topic of the policy is created too.

## PubSubTopic

| Field | Meaning |
| --- | --- |
| `name` | A short id or a full resource name. |
| `declare` | Create the topic at startup when it does not exist. |
| `labels` | Labels of a created topic. |
| `message_retention_duration` | How long a created topic keeps messages for seek. |

## PubSub

| Field | Meaning |
| --- | --- |
| `name` | A short id or a full resource name. |
| `declare` | Create the subscription, and its topic, at startup when they do not exist. |
| `ack_deadline_seconds` | The ack deadline of a created subscription, 10 to 600 seconds. |
| `enable_message_ordering` | Deliver messages of one [ordering key](publishing.md#ordering-keys) in publish order. |
| `enable_exactly_once_delivery` | Let an acknowledged message never be redelivered. See [exactly-once delivery](leases-and-flow-control.md#exactly-once-delivery). |
| `filter` | A filter expression on attributes. The service acknowledges messages it rejects and delivers none of them. |
| `dead_letter_policy` | A `DeadLetterPolicy`. |
| `retry_policy` | A `RetryPolicy`. |
| `message_retention_duration` | How long unacknowledged messages are kept. |
| `labels` | Labels of a created subscription. |

## DeadLetterPolicy

A message that fails too often moves to the dead-letter topic.

| Field | Meaning |
| --- | --- |
| `topic` | The dead-letter topic, a short id or a full name. |
| `max_delivery_attempts` | Attempts before the move, 5 to 100. The default is 5. |

## RetryPolicy

A nacked message is redelivered after a backoff.

| Field | Meaning |
| --- | --- |
| `minimum_backoff` | The shortest delay, 0 to 600 seconds. The default is 10 seconds. |
| `maximum_backoff` | The longest delay, 0 to 600 seconds. The default is 600 seconds. |

## Declaring by hand

`declare_topic` and `declare_subscription` create resources at a point you choose. Both ignore
a resource that exists and return the full resource name. The topic of a subscription must
exist.

```python title="service.py"
--8<-- "docs/docs_src/resources/manual.py"
```
