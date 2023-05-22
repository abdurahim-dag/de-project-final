from uuid import UUID
from pendulum import DateTime
from dataclasses import dataclass, fields
from decimal import Decimal


@dataclass
class Transaction:
    amount: int
    status: str
    country: str
    operation_id: UUID
    currency_code: str
    transaction_dt: DateTime
    transaction_type: str
    account_number_to: int
    account_number_from: int


@dataclass
class Currency:
    date_update: DateTime
    currency_code: str
    currency_with_div: Decimal
    currency_code_with: str

