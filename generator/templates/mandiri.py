"""Mandiri Faker generator — produces synthetic Mandiri transactions."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from faker import Faker

from schemas.v1.mandiri import MandiriTransaction

fake = Faker("id_ID")

_TRANSACTION_TYPES = ["TRANSFER", "PAYMENT", "DEPOSIT", "WITHDRAWAL"]
_CHANNELS = ["ATM", "MOBILE", "INTERNET", "BRANCH"]
_CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Yogyakarta"]


def generate_mandiri_transaction() -> dict[str, Any]:
    """Generate a single synthetic Mandiri transaction as a dict."""
    txn = MandiriTransaction(
        transaction_id=f"mandiri-{uuid.uuid4().hex[:12]}",
        user_id=fake.numerify("##########"),
        amount=Decimal(str(round(fake.pyfloat(min_value=10000, max_value=50000000, right_digits=2), 2))),
        transaction_timestamp=datetime.now(tz=timezone.utc),
        location_city=fake.random_element(_CITIES),
        no_rekening=fake.numerify("##########"),
        mandiri_channel=fake.random_element(_CHANNELS),
        transaction_type=fake.random_element(_TRANSACTION_TYPES),
    )
    return txn.model_dump(mode="json")
