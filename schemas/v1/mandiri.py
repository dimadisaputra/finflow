"""Mandiri (Bank Mandiri) transaction schema — v1."""

from typing import Literal

from schemas.v1.base import BaseTransaction


class MandiriTransaction(BaseTransaction):
    """Mandiri-specific transaction model.

    Extends ``BaseTransaction`` with Mandiri-specific fields such as
    account number, channel, and transaction type.
    """

    source: Literal["mandiri"] = "mandiri"
    schema_version: Literal["1.0"] = "1.0"
    no_rekening: str | None = None
    mandiri_channel: str | None = None  # e.g. "ATM", "MOBILE", "INTERNET"
    transaction_type: str  # e.g. "TRANSFER", "PAYMENT", "DEPOSIT"
