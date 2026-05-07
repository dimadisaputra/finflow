"""GoPay (Gojek e-wallet) transaction schema — v1."""

from typing import Literal

from schemas.v1.base import BaseTransaction


class GoPayTransaction(BaseTransaction):
    """GoPay-specific transaction model.

    Extends ``BaseTransaction`` with GoPay-specific fields such as
    merchant ID and payment method.
    """

    source: Literal["gopay"] = "gopay"
    schema_version: Literal["1.0"] = "1.0"
    gopay_merchant_id: str | None = None
    payment_method: str | None = None  # e.g. "QRIS", "DEEPLINK", "IN_APP"
    transaction_type: str  # e.g. "PAYMENT", "TOPUP", "TRANSFER"
