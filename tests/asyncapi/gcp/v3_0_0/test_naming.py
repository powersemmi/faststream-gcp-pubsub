import pytest

from faststream_gcp_pubsub import PubSubBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


class TestNaming(NamingTestCase):
    broker_class = PubSubBroker

    def test_base(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert schema == {
            "info": {"title": "FastStream", "version": "0.1.0"},
            "asyncapi": "3.0.0",
            "defaultContentType": "application/json",
            "servers": {
                "development": {
                    "host": "pubsub.googleapis.com:443",
                    "pathname": "",
                    "protocol": "googlepubsub",
                    "protocolVersion": "v1",
                }
            },
            "channels": {
                "test:Handle": {
                    "address": "test",
                    "description": "Reads subscription `test` of topic `test`.",
                    "servers": [{"$ref": "#/servers/development"}],
                    "messages": {
                        "SubscribeMessage": {
                            "$ref": "#/components/messages/test:Handle:SubscribeMessage"
                        }
                    },
                }
            },
            "operations": {
                "test:HandleSubscribe": {
                    "action": "receive",
                    "channel": {"$ref": "#/channels/test:Handle"},
                    "messages": [
                        {"$ref": "#/channels/test:Handle/messages/SubscribeMessage"}
                    ],
                }
            },
            "components": {
                "messages": {
                    "test:Handle:SubscribeMessage": {
                        "title": "test:Handle:SubscribeMessage",
                        "correlationId": {"location": "$message.header#/correlation_id"},
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    }
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }
