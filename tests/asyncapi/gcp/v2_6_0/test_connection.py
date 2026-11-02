import pytest
from faststream.specification import Tag
from google.auth.credentials import AnonymousCredentials

from faststream_gcp_pubsub import GCPSecurity, PubSubBroker
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema

# the documents are compared against the Google Cloud endpoint, not the machine's emulator
pytestmark = pytest.mark.usefixtures("no_ambient_emulator")


def test_base() -> None:
    broker = PubSubBroker(
        "project",
        emulator_host=None,
        description="Test description",
        tags=(Tag(name="some-tag", description="experimental"),),
    )
    schema = get_2_6_0_schema(broker)

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "description": "Test description",
                "protocol": "googlepubsub",
                "protocolVersion": "v1",
                "tags": [{"description": "experimental", "name": "some-tag"}],
                "url": "pubsub.googleapis.com:443",
            }
        },
    }, schema


def test_emulator_host_is_the_server_url() -> None:
    schema = get_2_6_0_schema(PubSubBroker("project", emulator_host="localhost:8085"))

    assert schema["servers"]["development"]["url"] == "localhost:8085"


def test_api_endpoint_is_the_server_url() -> None:
    schema = get_2_6_0_schema(
        PubSubBroker(
            "project",
            emulator_host=None,
            api_endpoint="europe-west1-pubsub.googleapis.com:443",
        )
    )

    assert (
        schema["servers"]["development"]["url"]
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
    schema = get_2_6_0_schema(broker)

    assert schema["servers"] == {
        "development": {
            "protocol": "custom",
            "protocolVersion": "2",
            "url": "pubsub.example.com:443",
        }
    }, schema


def test_security() -> None:
    broker = PubSubBroker(
        "project",
        emulator_host=None,
        security=GCPSecurity(credentials=AnonymousCredentials()),
    )
    schema = get_2_6_0_schema(broker)

    assert schema["servers"]["development"]["security"] == [{"google-oauth2": []}]
    assert schema["components"]["securitySchemes"] == {
        "google-oauth2": {"type": "oauth2"}
    }
