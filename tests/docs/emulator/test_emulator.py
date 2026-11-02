import json
from collections.abc import Callable
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.oauth2 import service_account

from docs.docs_src.emulator import credentials, emulator, endpoint
from faststream_gcp_pubsub import PubSubBroker
from faststream_gcp_pubsub.testing import TestPubSubBroker


@pytest.mark.asyncio()
async def test_emulator() -> None:
    async with TestPubSubBroker(emulator.broker) as br:
        await br.publish({"id": 1}, "orders")

        emulator.bill.mock.assert_called_once_with({"id": 1})


@pytest.fixture()
def key_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # the examples read `key.json` from the working directory, as a service run from its
    # own directory does; loading a key reads the file and parses the key, no network
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    (tmp_path / "key.json").write_text(
        json.dumps({
            "type": "service_account",
            "project_id": "my-project",
            "private_key_id": "key-1",
            "private_key": pem.decode(),
            "client_email": "billing@my-project.iam.gserviceaccount.com",
            "client_id": "1",
            "token_uri": "https://oauth2.googleapis.com/token",
        })
    )
    monkeypatch.chdir(tmp_path)


@pytest.mark.usefixtures("key_file")
@pytest.mark.parametrize(
    "build", (credentials.from_key_file, credentials.from_credentials)
)
def test_credentials_come_from_the_key_file(build: Callable[[], PubSubBroker]) -> None:
    broker = build()

    credentials = broker._connection_options.credentials
    assert isinstance(credentials, service_account.Credentials)
    assert (
        credentials.service_account_email == "billing@my-project.iam.gserviceaccount.com"
    )


def test_endpoint() -> None:
    assert endpoint.broker.project_id == "my-project"
