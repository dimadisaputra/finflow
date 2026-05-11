"""Schema registry — Pydantic v2 discriminated union for all transaction types.

This module defines ``AnyTransaction``, a discriminated union that Pydantic
uses to automatically dispatch validation to the correct model based on the
``schema_version`` field in the incoming payload.

More specific versions must come first in the Union so that Pydantic matches
them before falling back to a broader version.
"""

from typing import Annotated, Union

from pydantic import Field

from schemas.v1.bca import BCATransaction as BCAV1
from schemas.v1.gopay import GoPayTransaction as GoPayV1
from schemas.v1.mandiri import MandiriTransaction as MandiriV1
from schemas.v1.ovo import OVOTransaction as OVOV1
from schemas.v1.visa import VisaTransaction as VisaV1
from schemas.v2.bca import BCATransaction as BCAV2

# Pydantic v2 discriminated union — dispatches on source and schema_version.
# We nest the discriminated unions to handle multiple versions of the same source.
BCATransactions = Annotated[
    Union[BCAV2, BCAV1],
    Field(discriminator="schema_version")
]

AnyTransaction = Annotated[
    Union[BCATransactions, MandiriV1, GoPayV1, OVOV1, VisaV1],
    Field(discriminator="source"),
]
