"""BaseTransaction — shared fields across all transaction sources.

Every source-specific model inherits from this base.  Only source-specific
fields go in the subclass.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class BaseTransaction(BaseModel):
    """Base schema for all FinFlow transactions.

    Fields defined here are common to every transaction source (BCA, Mandiri,
    GoPay, OVO, Visa, etc.).  The ``schema_version`` field is used by
    Pydantic's discriminated union to dispatch validation to the correct
    model automatically.
    """

    transaction_id: str = Field(
        ...,
        description="Globally unique transaction identifier",
    )
    user_id: str = Field(
        ...,
        description="User identifier — FPE-tokenized before reaching the gateway",
    )
    amount: Decimal = Field(
        ...,
        ge=0,
        decimal_places=2,
        description="Transaction amount in the source currency",
    )
    transaction_timestamp: datetime = Field(
        ...,
        description="Timestamp when the transaction occurred at the source",
    )
    location_city: str = Field(
        ...,
        description="City where the transaction was initiated",
    )
    source: str = Field(
        ...,
        description="Source system identifier (e.g. 'bca', 'gopay')",
    )
    schema_version: str = Field(
        ...,
        description="Schema version string — used for discriminated union dispatch",
    )

    model_config = {
        "str_strip_whitespace": True,
        "json_schema_extra": {
            "examples": [
                {
                    "transaction_id": "txn-001",
                    "user_id": "usr-abc",
                    "amount": "150000.00",
                    "transaction_timestamp": "2026-01-15T10:30:00Z",
                    "location_city": "Jakarta",
                    "source": "bca",
                    "schema_version": "1.0",
                }
            ]
        },
    }
