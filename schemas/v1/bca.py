"""BCA (Bank Central Asia) transaction schema — v1."""

from typing import Literal

from schemas.v1.base import BaseTransaction


class BCATransaction(BaseTransaction):
    """BCA-specific transaction model.

    Extends ``BaseTransaction`` with BCA-specific fields such as
    terminal ID and transaction type.
    """

    source: Literal["bca"] = "bca"
    schema_version: Literal["1.0"] = "1.0"
    no_rekening: str | None = None
    bca_terminal_id: str | None = None
    transaction_type: str  # e.g. "TRANSFER", "PAYMENT", "WITHDRAWAL"
