import pytest


@pytest.fixture()
def no_ambient_emulator(monkeypatch: pytest.MonkeyPatch) -> None:
    # The server url is the emulator host when one is configured; documents are compared
    # against the Google Cloud endpoint, so the environment of the machine must not leak in.
    monkeypatch.delenv("PUBSUB_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
