from typing import TYPE_CHECKING, Any

from faststream.security import BaseSecurity
from typing_extensions import override

if TYPE_CHECKING:
    from google.auth.credentials import Credentials

__all__ = ("GCPSecurity",)


class GCPSecurity(BaseSecurity):
    """Google credentials the broker authenticates with.

    Without a security object the broker uses Application Default Credentials (the
    ``GOOGLE_APPLICATION_CREDENTIALS`` file, the gcloud login, or the metadata server).

    Examples:
        >>> def from_credentials() -> PubSubBroker:
        ...     credentials = service_account.Credentials.from_service_account_file("key.json")
        ...     return PubSubBroker(project_id="my-project", security=GCPSecurity(credentials))
    """

    __slots__ = ("credentials",)

    def __init__(self, credentials: "Credentials") -> None:
        super().__init__(use_ssl=True)
        self.credentials = credentials

    @classmethod
    def from_service_account_file(cls, filename: str) -> "GCPSecurity":
        """Credentials of a service account from its JSON key file."""
        from google.oauth2 import service_account  # ruff: ignore[import-outside-top-level]

        return cls(service_account.Credentials.from_service_account_file(filename))

    @classmethod
    def from_service_account_info(cls, info: dict[str, Any]) -> "GCPSecurity":
        """Credentials of a service account from its parsed JSON key."""
        from google.oauth2 import service_account  # ruff: ignore[import-outside-top-level]

        return cls(service_account.Credentials.from_service_account_info(info))

    @override
    def get_requirement(self) -> list[dict[str, Any]]:
        return [{"google-oauth2": []}]

    @override
    def get_schema(self) -> dict[str, dict[str, str]]:
        return {"google-oauth2": {"type": "oauth2"}}


def parse_security(security: BaseSecurity | None) -> "Credentials | None":
    """The credentials a security object carries; ``None`` selects the default ones."""
    if security is None:
        return None
    if isinstance(security, GCPSecurity):
        return security.credentials
    msg = f"PubSubBroker does not support {type(security).__name__}; use GCPSecurity"
    raise NotImplementedError(msg)
