import pytest

from faststream_gcp_pubsub import PubSubBroker
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase

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
            "asyncapi": "2.6.0",
            "defaultContentType": "application/json",
            "servers": {
                "development": {
                    "url": "pubsub.googleapis.com:443",
                    "protocol": "googlepubsub",
                    "protocolVersion": "v1",
                }
            },
            "channels": {
                "test:Handle": {
                    "description": "Reads subscription `test` of topic `test`.",
                    "servers": ["development"],
                    "publish": {
                        "message": {"$ref": "#/components/messages/test:Handle:Message"},
                    },
                }
            },
            "components": {
                "messages": {
                    "test:Handle:Message": {
                        "title": "test:Handle:Message",
                        "correlationId": {"location": "$message.header#/correlation_id"},
                        "payload": {"$ref": "#/components/schemas/EmptyPayload"},
                    }
                },
                "schemas": {"EmptyPayload": {"title": "EmptyPayload", "type": "null"}},
            },
        }
