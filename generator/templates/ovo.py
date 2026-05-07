"""OVO Faker generator — produces synthetic OVO transactions."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from faker import Faker

from schemas.v1.ovo import OVOTransaction

fake = Faker("id_ID")

_TRANSACTION_TYPES = ["PAYMENT", "TOPUP", "TRANSFER"]
_MERCHANT_NAMES = ["Tokopedia", "Grab", "Shopee", "Bukalapak", "Alfamart", "Indomaret"]
_CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Yogyakarta"]


def generate_ovo_transaction() -> dict[str, Any]:
    """Generate a single synthetic OVO transaction as a dict."""
    txn = OVOTransaction(
        transaction_id=f"ovo-{uuid.uuid4().hex[:12]}",
        user_id=fake.numerify("##########"),
        amount=Decimal(str(round(fake.pyfloat(min_value=1000, max_value=5000000, right_digits=2), 2))),
        transaction_timestamp=datetime.now(tz=timezone.utc),
        location_city=fake.random_element(_CITIES),
        ovo_merchant_name=fake.random_element(_MERCHANT_NAMES),
        cashback_amount=round(fake.pyfloat(min_value=0, max_value=50000, right_digits=2), 2) if fake.boolean(chance_of_getting_true=30) else None,
        transaction_type=fake.random_element(_TRANSACTION_TYPES),
    )
    return txn.model_dump(mode="json")
