"""Visa Faker generator — produces synthetic Visa credit card transactions."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from faker import Faker

from schemas.v1.visa import VisaTransaction

fake = Faker("id_ID")

_TRANSACTION_TYPES = ["PURCHASE", "REFUND", "CASH_ADVANCE"]
_MERCHANT_NAMES = ["Amazon", "Tokopedia", "Shopee", "Apple Store", "Google Play", "Grab", "GoFood"]
_MCCS = ["5411", "5812", "5912", "7011", "5541", "5311"]  # ISO 18245 MCCs
_CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Singapore", "Kuala Lumpur", "Tokyo"]


def generate_visa_transaction() -> dict[str, Any]:
    """Generate a single synthetic Visa transaction as a dict."""
    txn = VisaTransaction(
        transaction_id=f"visa-{uuid.uuid4().hex[:12]}",
        user_id=fake.numerify("##########"),
        amount=Decimal(str(round(fake.pyfloat(min_value=10000, max_value=100000000, right_digits=2), 2))),
        transaction_timestamp=datetime.now(tz=timezone.utc),
        location_city=fake.random_element(_CITIES),
        card_last_four=fake.numerify("####"),
        merchant_category_code=fake.random_element(_MCCS),
        merchant_name=fake.random_element(_MERCHANT_NAMES),
        transaction_type=fake.random_element(_TRANSACTION_TYPES),
    )
    return txn.model_dump(mode="json")
