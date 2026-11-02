import pytest
from faststream.specification import Tag
from google.auth.credentials import AnonymousCredentials

from faststream_gcp_pubsub import GCPSecurity, PubSubBroker
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


def test_base() -> None:
    broker = PubSubBroker(
        "project",
        emulator_host=None,
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = get_3_0_0_schema(broker)

    assert schema == {
        "info": {"title": "FastStream", "version": "0.1.0"},
        "asyncapi": "3.0.0",
        "defaultContentType": "application/json",
        "servers": {
            "development": {
                "host": "pubsub.googleapis.com:443",
                "pathname": "",
                "protocol": "googlepubsub",
                "description": "Test description",
                "protocolVersion": "v1",
                "tags": [{"name": "some-tag", "description": "experimental"}],
            }
        },
        "channels": {},
        "operations": {},
        "components": {"messages": {}, "schemas": {}},
    }, schema


def test_emulator_host_is_the_server_host() -> None:
    schema = get_3_0_0_schema(PubSubBroker("project", emulator_host="localhost:8085"))

    assert schema["servers"]["development"]["host"] == "localhost:8085"


def test_api_endpoint_is_the_server_host() -> None:
    schema = get_3_0_0_schema(
        PubSubBroker(
            "project",
            emulator_host=None,
            api_endpoint="europe-west1-pubsub.googleapis.com:443",
        )
    )

    assert (
        schema["servers"]["development"]["host"]
        == "europe-west1-pubsub.googleapis.com:443"
    )


def test_custom() -> None:
    broker = PubSubBroker(
        "project",
        emulator_host="localhost:8085",
        specification_url="pubsub.example.com:443",
        protocol="custom",
        protocol_version="2",
    )
    schema = get_3_0_0_schema(broker)

    assert schema["servers"] == {
        "development": {
            "host": "pubsub.example.com:443",
            "pathname": "",
            "protocol": "custom",
            "protocolVersion": "2",
        }
    }, schema


def test_security() -> None:
    broker = PubSubBroker(
        "project",
        emulator_host=None,
        security=GCPSecurity(credentials=AnonymousCredentials()),
    )
    schema = get_3_0_0_schema(broker)

    assert schema["servers"]["development"]["security"] == [
        {"$ref": "#/components/securitySchemes/google-oauth2"}
    ]
    assert schema["components"]["securitySchemes"] == {
        "google-oauth2": {"type": "oauth2"}
    }
