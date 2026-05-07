"""GoPay Faker generator — produces synthetic GoPay transactions."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from faker import Faker

from schemas.v1.gopay import GoPayTransaction

fake = Faker("id_ID")

_TRANSACTION_TYPES = ["PAYMENT", "TOPUP", "TRANSFER"]
_PAYMENT_METHODS = ["QRIS", "DEEPLINK", "IN_APP"]
_CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Yogyakarta", "Denpasar"]


def generate_gopay_transaction() -> dict[str, Any]:
    """Generate a single synthetic GoPay transaction as a dict."""
    txn = GoPayTransaction(
        transaction_id=f"gopay-{uuid.uuid4().hex[:12]}",
        user_id=fake.numerify("##########"),
        amount=Decimal(str(round(fake.pyfloat(min_value=1000, max_value=5000000, right_digits=2), 2))),
        transaction_timestamp=datetime.now(tz=timezone.utc),
        location_city=fake.random_element(_CITIES),
        gopay_merchant_id=fake.bothify("GOPAY-M-????-####"),
        payment_method=fake.random_element(_PAYMENT_METHODS),
        transaction_type=fake.random_element(_TRANSACTION_TYPES),
    )
    return txn.model_dump(mode="json")
