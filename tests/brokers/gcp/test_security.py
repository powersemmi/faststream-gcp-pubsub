import json
from pathlib import Path
from typing import Any

import pytest
from faststream.exceptions import IncorrectState
from faststream.security import SASLPlaintext
from google.auth.credentials import AnonymousCredentials
from google.oauth2 import service_account

from faststream_gcp_pubsub import GCPSecurity, PubSubBroker


def test_security_carries_credentials() -> None:
    credentials = AnonymousCredentials()

    broker = PubSubBroker(project_id="p", security=GCPSecurity(credentials))

    assert broker._connection_options.credentials is credentials


def service_account_key() -> dict[str, Any]:
    """A service account JSON key, as the console downloads it."""
    rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return {
        "type": "service_account",
        "project_id": "p",
        "private_key_id": "key-1",
        "private_key": pem.decode(),
        "client_email": "svc@p.iam.gserviceaccount.com",
        "token_uri": "https://oauth2.googleapis.com/token",
    }


def test_security_from_service_account_info() -> None:
    security = GCPSecurity.from_service_account_info(service_account_key())

    broker = PubSubBroker(project_id="p", security=security)

    credentials = broker._connection_options.credentials
    assert isinstance(credentials, service_account.Credentials)
    assert credentials.service_account_email == "svc@p.iam.gserviceaccount.com"


def test_security_from_service_account_file(tmp_path: Path) -> None:
    key_file = tmp_path / "key.json"
    key_file.write_text(json.dumps(service_account_key()))

    broker = PubSubBroker(
        project_id="p", security=GCPSecurity.from_service_account_file(str(key_file))
    )

    credentials = broker._connection_options.credentials
    assert isinstance(credentials, service_account.Credentials)
    assert credentials.service_account_email == "svc@p.iam.gserviceaccount.com"


def test_security_is_in_the_specification() -> None:
    broker = PubSubBroker(project_id="p", security=GCPSecurity(AnonymousCredentials()))

    assert broker.specification.security is not None
    assert broker.specification.security.get_schema() == {
        "google-oauth2": {"type": "oauth2"}
    }


def test_foreign_security_is_refused() -> None:
    with pytest.raises(NotImplementedError, match="GCPSecurity"):
        PubSubBroker(project_id="p", security=SASLPlaintext(username="u", password="p"))


def test_emulator_host_comes_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:9999")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "from-env")

    broker = PubSubBroker()

    assert broker._connection_options.emulator_host == "localhost:9999"
    assert broker.project_id == "from-env"


def test_explicit_none_ignores_the_emulator_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "localhost:9999")

    broker = PubSubBroker(project_id="p", emulator_host=None)

    assert broker._connection_options.emulator_host is None
    assert broker.specification.url == ["pubsub.googleapis.com:443"]


@pytest.mark.asyncio()
async def test_connect_without_project_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    broker = PubSubBroker(emulator_host="localhost:1")

    with pytest.raises(IncorrectState, match="project_id"):
        await broker.connect()
