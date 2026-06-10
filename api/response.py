"""Unified API response models and helpers."""

from __future__ import annotations

from typing import Any

from fastapi.responses import JSONResponse
from pydantic import BaseModel


class ApiResponse(BaseModel):
    """Standard envelope for all API responses."""

    success: bool = True
    code: str = "OK"
    message: str = ""
    data: Any = None
    meta: dict[str, Any] = {}


def ok_response(data: Any = None, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a success response dict."""
    return ApiResponse(
        success=True,
        code="OK",
        message="",
        data=data,
        meta=meta or {},
    ).model_dump()


def error_response(
    code: str,
    message: str,
    status_code: int = 400,
) -> JSONResponse:
    """Build an error JSONResponse."""
    body = ApiResponse(
        success=False,
        code=code,
        message=message,
    ).model_dump()
    return JSONResponse(content=body, status_code=status_code)
