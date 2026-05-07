"""BCA Faker generator — produces synthetic BCA transactions.

Imports the model from ``schemas/`` to ensure compliance with the
data contract.  Never define payload structure here.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from faker import Faker

from schemas.v1.bca import BCATransaction

fake = Faker("id_ID")

_TRANSACTION_TYPES = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT"]
_CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Yogyakarta"]


def generate_bca_transaction() -> dict[str, Any]:
    """Generate a single synthetic BCA transaction as a dict."""
    txn = BCATransaction(
        transaction_id=f"bca-{uuid.uuid4().hex[:12]}",
        user_id=fake.numerify("##########"),
        amount=Decimal(str(round(fake.pyfloat(min_value=10000, max_value=50000000, right_digits=2), 2))),
        transaction_timestamp=datetime.now(tz=timezone.utc),
        location_city=fake.random_element(_CITIES),
        no_rekening=fake.numerify("##########"),
        bca_terminal_id=fake.numerify("BCA-TERM-####"),
        transaction_type=fake.random_element(_TRANSACTION_TYPES),
    )
    return txn.model_dump(mode="json")
