# API reference

## Broker

::: faststream_gcp_pubsub.PubSubBroker
    options:
      members:
        - __init__
        - project_id
        - declare_topic
        - declare_subscription
        - publish
        - publish_batch
        - request
        - ping

## Resources

::: faststream_gcp_pubsub.PubSubTopic

::: faststream_gcp_pubsub.PubSub

::: faststream_gcp_pubsub.DeadLetterPolicy

::: faststream_gcp_pubsub.RetryPolicy

## Consuming

::: faststream_gcp_pubsub.FlowControl

::: faststream_gcp_pubsub.PubSubMessage

## Publishing

::: faststream_gcp_pubsub.PubSubResponse

## Routers

::: faststream_gcp_pubsub.PubSubRouter

::: faststream_gcp_pubsub.PubSubRoute

::: faststream_gcp_pubsub.PubSubPublisher

## Security

::: faststream_gcp_pubsub.GCPSecurity

## Testing

::: faststream_gcp_pubsub.testing.TestPubSubBroker

## Integrations

::: faststream_gcp_pubsub.fastapi.PubSubRouter

::: faststream_gcp_pubsub.opentelemetry.PubSubTelemetryMiddleware

::: faststream_gcp_pubsub.prometheus.PubSubPrometheusMiddleware
