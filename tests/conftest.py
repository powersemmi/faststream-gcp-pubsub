import asyncio
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "flaky: retry a test that depends on timing")


@pytest.fixture()
def queue() -> str:
    # Pub/Sub resource ids start with a letter; a bare UUID may not.
    return f"q-{uuid4()}"


@pytest.fixture()
def event() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture()
def event2() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture()
def event3() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture()
def mock() -> Generator[MagicMock, Any, None]:
    """Should be generator to share mock between tests."""
    m = MagicMock()
    yield m
    m.reset_mock()


@pytest.fixture()
def mock2() -> Generator[MagicMock, Any, None]:
    """Should be generator to share mock between tests."""
    m = MagicMock()
    yield m
    m.reset_mock()
