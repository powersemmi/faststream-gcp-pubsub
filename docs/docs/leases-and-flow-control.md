# Leases and flow control

The subscriber reads through a streaming pull and keeps each message leased: it extends the ack
deadline of every message in work until the handler settles it, up to `max_lease`.
`FlowControl` bounds what the subscriber holds at once.

```python title="service.py"
--8<-- "docs/docs_src/leases/flow_control.py"
```

| Field | Default | Meaning |
| --- | --- | --- |
| `max_messages` | 1000 | Deliveries held at once, unsettled. The stream is not read further until one settles. At least 1. |
| `max_bytes` | 100 MiB | Payload bytes held at once. The service stops sending past it. |
| `ack_deadline` | 60 s | The length of a lease before it is extended, 10 to 600. |
| `max_lease` | 3600 s | The longest a delivery is held. Past it the lease is no longer extended and the service redelivers the message. |
| `ack_batch_wait` | 0.05 s | How long acks and nacks wait to be sent together. |

Choose `max_lease` above the longest time a handler can take. A handler that outlives it loses
its message to a redelivery.

## Batched settlement

Acks and nacks of one subscription are sent together: one request per `ack_batch_wait`, or per
1000 ids, which is the largest request the service takes. The handler does not wait for the
request.

## Exactly-once delivery

On a subscription with `enable_exactly_once_delivery=True` the service guarantees that an acknowledged
message is not delivered again. There `ack()` waits for the service's confirmation and raises when the service refuses it.

```python title="service.py"
--8<-- "docs/docs_src/leases/exactly_once.py"
```

With the default policy the confirmation failure follows the usual path: the handler's message
is nacked when the exception propagates.

## Graceful shutdown

On shutdown the subscriber stops reading, waits for the handlers in work up to the broker's
`graceful_timeout` (15 seconds by default), sends the pending acks and returns the messages it
still holds to the service, which delivers them to another consumer.

```python title="service.py"
--8<-- "docs/docs_src/leases/flow_control.py:broker"
```
