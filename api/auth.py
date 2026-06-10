"""X-API-Token authentication dependency for FastAPI."""

from __future__ import annotations

from fastapi import Depends, Header, HTTPException, status

from YM_data_collection.config.loader import resolve_secret
from YM_data_collection.config.models import AuthConfig


def create_auth_dependency(auth: AuthConfig):
    """Return a FastAPI dependency that validates X-API-Token.

    If ``auth.enabled`` is False the dependency is a no-op and always succeeds.
    """

    async def verify_token(x_api_token: str | None = Header(default=None)) -> str:
        if not auth.enabled:
            return ""

        if x_api_token is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing X-API-Token header",
            )

        try:
            expected = resolve_secret(auth.http_token_secret_ref)
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Server auth misconfiguration",
            )

        if x_api_token != expected:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API token",
            )

        return x_api_token

    return verify_token
