# FastAPI

`PubSubRouter` from `faststream_gcp_pubsub.fastapi` is a FastAPI router that owns a broker. It
takes the options of `PubSubBroker` and of `fastapi.APIRouter`.

```python title="app.py"
--8<-- "docs/docs_src/fastapi/app.py"
```

Subscribers start with the FastAPI application and stop with it. `router.broker` is the
`PubSubBroker`, so `router.broker.publish(...)` publishes from a route.

The broker's `dependencies` are FastAPI `dependencies` here, and its `tags` are
`specification_tags`.

## Testing

Test the router with the broker it owns:

```python title="test_app.py"
--8<-- "tests/docs/fastapi/test_app.py:test"
```

The module also exports `PubSubMessage`, `PubSubBroker`, `Connection`, `Context`, `ContextRepo`
and `Logger` as FastAPI-aware annotations for handler arguments.
