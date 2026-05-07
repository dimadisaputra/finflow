"""BCA transaction schema — v2 (extends v1 with merchant_category)."""

from typing import Literal

from schemas.v1.bca import BCATransaction as BCATransactionV1


class BCATransaction(BCATransactionV1):
    """BCA v2 — adds ``merchant_category`` to the v1 model.

    Inherits all fields from v1.  The ``schema_version`` discriminator
    is updated to ``"2.0"`` so Pydantic dispatches correctly.
    """

    schema_version: Literal["2.0"] = "2.0"
    merchant_category: str | None = None  # New field in v2
