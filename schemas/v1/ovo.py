"""OVO (e-wallet) transaction schema — v1."""

from typing import Literal

from schemas.v1.base import BaseTransaction


class OVOTransaction(BaseTransaction):
    """OVO-specific transaction model.

    Extends ``BaseTransaction`` with OVO-specific fields such as
    merchant name and cashback amount.
    """

    source: Literal["ovo"] = "ovo"
    schema_version: Literal["1.0"] = "1.0"
    ovo_merchant_name: str | None = None
    cashback_amount: float | None = None
    transaction_type: str  # e.g. "PAYMENT", "TOPUP", "TRANSFER"
