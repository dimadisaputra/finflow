"""Visa credit card transaction schema — v1."""

from typing import Literal

from schemas.v1.base import BaseTransaction


class VisaTransaction(BaseTransaction):
    """Visa-specific transaction model.

    Extends ``BaseTransaction`` with credit-card-specific fields such as
    merchant category code (MCC) and card last four digits.
    """

    source: Literal["visa"] = "visa"
    schema_version: Literal["1.0"] = "1.0"
    card_last_four: str | None = None
    merchant_category_code: str | None = None  # ISO 18245 MCC
    merchant_name: str | None = None
    transaction_type: str  # e.g. "PURCHASE", "REFUND", "CASH_ADVANCE"
