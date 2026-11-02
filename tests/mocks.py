# Vendored from FastStream (https://github.com/ag2ai/faststream), tests/mocks.py, Apache-2.0.
# Adapted for faststream-gcp-pubsub; see tests/NOTICE.

from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import Any
from unittest.mock import Mock

from pytest import MonkeyPatch  # ruff: ignore[pytest-incorrect-pytest-import]


@contextmanager
def mock_pydantic_settings_env(
    env_mapping: Mapping[str, Any],
) -> Generator[None, None, None]:
    with MonkeyPatch().context() as c:
        mock = Mock()
        mock.return_value = env_mapping
        c.setattr(
            "pydantic_settings.sources.DotEnvSettingsSource._read_env_files",
            mock,
        )
        yield
