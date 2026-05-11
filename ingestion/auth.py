"""Per-source API key verification for the ingestion gateway.

Each transaction source (BCA, Mandiri, GoPay, etc.) has its own API key
stored in Vault.  This module provides a FastAPI dependency that validates
the ``X-API-Key`` header against the expected key for the given source.
"""

from __future__ import annotations

import logging
import os

from fastapi import Header, HTTPException, Request

logger = logging.getLogger(__name__)

# Valid source names — must match Redpanda topic suffixes.
VALID_SOURCES: set[str] = {"bca", "mandiri", "gopay", "ovo", "visa"}


def _get_api_key(source: str) -> str:
    """Retrieve the expected API key for a given source from environment.

    Keys are injected by the Vault agent sidecar as environment variables
    following the pattern ``FINFLOW_API_KEY_<SOURCE_UPPER>``.

    Raises
    ------
    HTTPException
        If no key is configured for the given source.
    """
    env_var = f"FINFLOW_API_KEY_{source.upper()}"
    key = os.environ.get(env_var)
    if not key:
        logger.error("No API key configured for source '%s' (env: %s)", source, env_var)
        raise HTTPException(
            status_code=500,
            detail=f"Server misconfiguration: no API key for source '{source}'",
        )
    return key


async def verify_source_token(
    request: Request,
    x_api_key: str = Header(..., description="Source-specific API key"),
) -> None:
    """FastAPI dependency — verify the API key for the requested source.

    The source name is extracted from the path parameter.

    Raises
    ------
    HTTPException
        401 if the key is missing or does not match.
    """
    source: str = request.path_params.get("source", "")

    if source not in VALID_SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source '{source}'. Valid sources: {sorted(VALID_SOURCES)}",
        )

    expected_key = _get_api_key(source)
    if x_api_key != expected_key:
        logger.warning("Invalid API key for source '%s'", source)
        raise HTTPException(status_code=401, detail="Invalid API key")
