"""FPE (Format-Preserving Encryption) tokenization for PII fields.

Uses FF3-1 (AES-FFX) to deterministically tokenize ``user_id`` and
``no_rekening`` fields before they enter Redpanda.  Determinism is
critical — the streaming fraud detection jobs aggregate by ``user_id``
using sliding-window GROUP BY.  If the cipher key changes between
messages, the same user will produce different tokens and velocity
checks will silently fail.

Key rotation requires a full pipeline stop + re-tokenization of
existing data.  See ``docs/pii-key-lifecycle.yml``.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

from ff3 import FF3Cipher

logger = logging.getLogger(__name__)

# PII fields that require FPE tokenization at the ingestion layer.
_PII_FIELDS: set[str] = {"user_id", "no_rekening"}


@lru_cache(maxsize=1)
def _get_cipher() -> FF3Cipher:
    """Load the FF3 cipher once at startup and cache forever.

    The key and tweak are injected by the Vault agent sidecar as
    environment variables.  Never reload mid-run — see module docstring.
    """
    key = os.environ["FPE_KEY"]
    tweak = os.environ["FPE_TWEAK"]
    return FF3Cipher.withCustomAlphabet(key, tweak, alphabet="0123456789abcdefghijklmnopqrstuvwxyz")


def tokenize_pii(
    payload: dict[str, Any],
    *,
    source: str,
) -> dict[str, Any]:
    """Apply FPE tokenization to PII fields in the payload.

    Parameters
    ----------
    payload : dict
        The deserialized transaction payload (from ``model_dump``).
    source : str
        The source name (for logging context).

    Returns
    -------
    dict
        A shallow copy of ``payload`` with PII fields replaced by their
        tokenized values.
    """
    cipher = _get_cipher()
    result = dict(payload)

    for field in _PII_FIELDS:
        raw_value = result.get(field)
        if raw_value is not None and raw_value != "":
            try:
                result[field] = cipher.encrypt(str(raw_value))
            except Exception:
                # Never log the raw PII value
                logger.exception(
                    "FPE tokenization failed for field '%s' in source '%s'",
                    field,
                    source,
                )
                raise

    return result
