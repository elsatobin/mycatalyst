from typing import Any, Optional, Type
import uuid
import datetime

from .postgres.tables import _Column
from .postgres import column_types as pg_types


class Text(_Column[str]):
    def __init__(self):
        super().__init__(pg_types.Text())


class Varchar(_Column[str]):
    def __init__(self, length: int):
        super().__init__(pg_types.Varchar(length))


class Integer(_Column[int]):
    def __init__(self):
        super().__init__(pg_types.Integer())


class BigInteger(_Column[int]):
    def __init__(self):
        super().__init__(pg_types.BigInteger())


class UUID(_Column[uuid.UUID]):
    def __init__(self):
        super().__init__(pg_types.UUID())


class JSONB(_Column[Any]):
    def __init__(self):
        super().__init__(pg_types.JSONB())


class Boolean(_Column[bool]):
    def __init__(self):
        super().__init__(pg_types.Boolean())


class Timestamp(_Column[datetime.datetime]):
    def __init__(self, with_timezone: bool = False):
        super().__init__(pg_types.Timestamp(with_timezone))


class Date(_Column[datetime.date]):
    def __init__(self):
        super().__init__(pg_types.Date())


class Time(_Column[datetime.time]):
    def __init__(self, with_timezone: bool = False):
        super().__init__(pg_types.Time(with_timezone))


class Serial(_Column[int]):
    def __init__(self):
        super().__init__(pg_types.Serial())


class BigSerial(_Column[int]):
    def __init__(self):
        super().__init__(pg_types.BigSerial())


class Numeric(_Column[float]):
    def __init__(self, precision: Optional[int] = None, scale: Optional[int] = None):
        super().__init__(pg_types.Numeric(precision, scale))
