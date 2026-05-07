"""Schema validation tests for all transaction models.

Verifies that:
- All v1 models properly inherit from BaseTransaction
- Source-specific fields are present and validated
- Discriminated union (AnyTransaction) dispatches correctly
- Invalid payloads are rejected
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from schemas.registry import AnyTransaction
from schemas.v1.base import BaseTransaction
from schemas.v1.bca import BCATransaction
from schemas.v1.gopay import GoPayTransaction
from schemas.v1.mandiri import MandiriTransaction
from schemas.v1.ovo import OVOTransaction
from schemas.v1.visa import VisaTransaction
from schemas.v2.bca import BCATransaction as BCATransactionV2


class TestBaseTransaction:
    """Tests for BaseTransaction model."""

    def test_valid_base_transaction(self) -> None:
        txn = BaseTransaction(
            transaction_id="txn-001",
            user_id="usr-abc",
            amount=Decimal("150000.00"),
            transaction_timestamp=datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc),
            location_city="Jakarta",
            source="bca",
            schema_version="1.0",
        )
        assert txn.transaction_id == "txn-001"
        assert txn.amount == Decimal("150000.00")

    def test_negative_amount_rejected(self) -> None:
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            BaseTransaction(
                transaction_id="txn-001",
                user_id="usr-abc",
                amount=Decimal("-100.00"),
                transaction_timestamp=datetime.now(tz=timezone.utc),
                location_city="Jakarta",
                source="bca",
                schema_version="1.0",
            )

    def test_missing_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            BaseTransaction(
                transaction_id="txn-001",
                # Missing user_id, amount, etc.
            )


class TestBCATransaction:
    """Tests for BCA transaction model."""

    def test_valid_bca_transaction(self) -> None:
        txn = BCATransaction(
            transaction_id="bca-001",
            user_id="1234567890",
            amount=Decimal("500000.00"),
            transaction_timestamp=datetime.now(tz=timezone.utc),
            location_city="Jakarta",
            no_rekening="0987654321",
            bca_terminal_id="BCA-TERM-0001",
            transaction_type="TRANSFER",
        )
        assert txn.source == "bca"
        assert txn.schema_version == "1.0"
        assert isinstance(txn, BaseTransaction)

    def test_bca_inherits_base(self) -> None:
        assert issubclass(BCATransaction, BaseTransaction)


class TestBCATransactionV2:
    """Tests for BCA v2 transaction model."""

    def test_v2_extends_v1(self) -> None:
        txn = BCATransactionV2(
            transaction_id="bca-002",
            user_id="1234567890",
            amount=Decimal("750000.00"),
            transaction_timestamp=datetime.now(tz=timezone.utc),
            location_city="Bandung",
            no_rekening="0987654321",
            bca_terminal_id="BCA-TERM-0002",
            transaction_type="PAYMENT",
            merchant_category="Retail",
        )
        assert txn.schema_version == "2.0"
        assert txn.merchant_category == "Retail"
        assert isinstance(txn, BCATransaction)


class TestMandiriTransaction:
    """Tests for Mandiri transaction model."""

    def test_valid_mandiri_transaction(self) -> None:
        txn = MandiriTransaction(
            transaction_id="mandiri-001",
            user_id="1234567890",
            amount=Decimal("300000.00"),
            transaction_timestamp=datetime.now(tz=timezone.utc),
            location_city="Surabaya",
            no_rekening="1122334455",
            mandiri_channel="MOBILE",
            transaction_type="TRANSFER",
        )
        assert txn.source == "mandiri"
        assert txn.mandiri_channel == "MOBILE"


class TestGoPayTransaction:
    """Tests for GoPay transaction model."""

    def test_valid_gopay_transaction(self) -> None:
        txn = GoPayTransaction(
            transaction_id="gopay-001",
            user_id="1234567890",
            amount=Decimal("50000.00"),
            transaction_timestamp=datetime.now(tz=timezone.utc),
            location_city="Jakarta",
            gopay_merchant_id="GOPAY-M-abcd-1234",
            payment_method="QRIS",
            transaction_type="PAYMENT",
        )
        assert txn.source == "gopay"
        assert txn.payment_method == "QRIS"


class TestOVOTransaction:
    """Tests for OVO transaction model."""

    def test_valid_ovo_transaction(self) -> None:
        txn = OVOTransaction(
            transaction_id="ovo-001",
            user_id="1234567890",
            amount=Decimal("25000.00"),
            transaction_timestamp=datetime.now(tz=timezone.utc),
            location_city="Bandung",
            ovo_merchant_name="Tokopedia",
            cashback_amount=2500.0,
            transaction_type="PAYMENT",
        )
        assert txn.source == "ovo"
        assert txn.cashback_amount == 2500.0


class TestVisaTransaction:
    """Tests for Visa transaction model."""

    def test_valid_visa_transaction(self) -> None:
        txn = VisaTransaction(
            transaction_id="visa-001",
            user_id="1234567890",
            amount=Decimal("1500000.00"),
            transaction_timestamp=datetime.now(tz=timezone.utc),
            location_city="Singapore",
            card_last_four="4242",
            merchant_category_code="5411",
            merchant_name="Amazon",
            transaction_type="PURCHASE",
        )
        assert txn.source == "visa"
        assert txn.card_last_four == "4242"


class TestDiscriminatedUnion:
    """Tests for AnyTransaction discriminated union dispatch."""

    def test_dispatches_to_bca_v1(self) -> None:
        data = {
            "transaction_id": "bca-001",
            "user_id": "usr",
            "amount": "100.00",
            "transaction_timestamp": "2026-01-01T00:00:00Z",
            "location_city": "Jakarta",
            "source": "bca",
            "schema_version": "1.0",
            "transaction_type": "TRANSFER",
        }
        from pydantic import TypeAdapter

        adapter = TypeAdapter(AnyTransaction)
        txn = adapter.validate_python(data)
        assert isinstance(txn, BCATransaction)
        assert txn.schema_version == "1.0"

    def test_dispatches_to_bca_v2(self) -> None:
        data = {
            "transaction_id": "bca-002",
            "user_id": "usr",
            "amount": "200.00",
            "transaction_timestamp": "2026-01-01T00:00:00Z",
            "location_city": "Jakarta",
            "source": "bca",
            "schema_version": "2.0",
            "transaction_type": "PAYMENT",
            "merchant_category": "Retail",
        }
        from pydantic import TypeAdapter

        adapter = TypeAdapter(AnyTransaction)
        txn = adapter.validate_python(data)
        assert isinstance(txn, BCATransactionV2)
        assert txn.merchant_category == "Retail"
