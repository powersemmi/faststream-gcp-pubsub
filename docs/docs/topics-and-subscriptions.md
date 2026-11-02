# Topics and subscriptions

Pub/Sub keeps topics and subscriptions as separate resources. A publisher names a topic. A
subscriber names a subscription attached to a topic.

```python title="service.py"
--8<-- "docs/docs_src/topics/fan_out.py"
```

A message published to `orders` is delivered to both `orders-billing` and `orders-audit`. When
several consumers read one subscription, they share its messages.

## Names

A short id such as `orders` is expanded with the broker's project:
`projects/my-project/topics/orders`. A name that starts with `projects/` is a full name and is
used as it is. This is how a subscriber reads a subscription of another project.

A subscription named without a `topic` is attached to the topic of the same name. The `topic`
argument names what the subscription is attached to. The broker uses it to declare the
resources, in the test broker routing and in the AsyncAPI document.

## Names as declarations

Both arguments also take a declaration, [`PubSubSubscription`](declaring-resources.md) and
`PubSubTopic`, which describes how to create the resource:

```python title="service.py"
--8<-- "docs/docs_src/topics/declarations.py"
```

See [Declaring resources](declaring-resources.md) for every setting.
