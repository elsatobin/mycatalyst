from typing import Any, Optional, Type
import uuid
import datetime


class PostgresType:
    def __init__(self, sql_type_name: str):
        self.sql_type_name = sql_type_name

    @property
    def python_type(self) -> Type:
        """The Python type this SQL type maps to."""
        raise NotImplementedError("Subclasses must implement python_type")

    def extends_column(self, column):
        pass


class Text(PostgresType):
    """PostgreSQL TEXT type."""

    def __init__(self):
        super().__init__("TEXT")

    @property
    def python_type(self) -> Type[str]:
        return str

    def extends_column(self, column):
        pass


class Varchar(PostgresType):
    """PostgreSQL VARCHAR type."""

    def __init__(self, length: int):
        super().__init__(f"VARCHAR({length})")
        self.length = length

    @property
    def python_type(self) -> Type[str]:
        return str

    def extends_column(self, column):
        pass


class Integer(PostgresType):
    """PostgreSQL INTEGER type."""

    def __init__(self):
        super().__init__("INTEGER")

    @property
    def python_type(self) -> Type[int]:
        return int

    def extends_column(self, column):
        pass


class BigInteger(PostgresType):
    """PostgreSQL BIGINT type."""

    def __init__(self):
        super().__init__("BIGINT")

    @property
    def python_type(self) -> Type[int]:
        return int

    def extends_column(self, column):
        pass


class UUID(PostgresType):
    """PostgreSQL UUID type."""

    def __init__(self):
        super().__init__("UUID")

    @property
    def python_type(self) -> Type[uuid.UUID]:
        return uuid.UUID

    def extends_column(self, column):
        """Add UUID-specific methods."""

        def uuid_method(col):
            col._default_expr = "gen_random_uuid()"
            col._has_default = True
            return col

        column.uuid = uuid_method.__get__(column)


class JSONB(PostgresType):
    """PostgreSQL JSONB type."""

    def __init__(self):
        super().__init__("JSONB")

    @property
    def python_type(self) -> Type[Any]:
        return dict

    def extends_column(self, column):
        pass


class Boolean(PostgresType):
    """PostgreSQL BOOLEAN type."""

    def __init__(self):
        super().__init__("BOOLEAN")

    @property
    def python_type(self) -> Type[bool]:
        return bool

    def extends_column(self, column):
        pass


class Timestamp(PostgresType):
    """PostgreSQL TIMESTAMP type."""

    def __init__(self, with_timezone: bool = False):
        super().__init__(
            "TIMESTAMP WITH TIME ZONE"
            if with_timezone
            else "TIMESTAMP WITHOUT TIME ZONE"
        )
        self.with_timezone = with_timezone

    @property
    def python_type(self) -> Type[datetime.datetime]:
        return datetime.datetime

    def extends_column(self, column):
        """Add timestamp-specific methods."""

        def now_method(col):
            col._default_expr = "NOW()"
            col._has_default = True
            return col

        column.now = now_method.__get__(column)


class Date(PostgresType):
    """PostgreSQL DATE type."""

    def __init__(self):
        super().__init__("DATE")

    @property
    def python_type(self) -> Type[datetime.date]:
        return datetime.date

    def extends_column(self, column):
        """Add date-specific methods."""

        def today_method(col):
            col._default_expr = "CURRENT_DATE"
            col._has_default = True
            return col

        column.today = today_method.__get__(column)


class Time(PostgresType):
    """PostgreSQL TIME type."""

    def __init__(self, with_timezone: bool = False):
        super().__init__(
            "TIME WITH TIME ZONE" if with_timezone else "TIME WITHOUT TIME ZONE"
        )
        self.with_timezone = with_timezone

    @property
    def python_type(self) -> Type[datetime.time]:
        return datetime.time

    def extends_column(self, column):
        pass


class Serial(PostgresType):
    """PostgreSQL SERIAL type for auto-incrementing IDs."""

    def __init__(self):
        super().__init__("SERIAL")

    @property
    def python_type(self) -> Type[int]:
        return int

    def extends_column(self, column):
        """Add serial-specific methods."""
        column._has_default = True
        column._default_expr = None


class BigSerial(PostgresType):
    """PostgreSQL BIGSERIAL type for large auto-incrementing IDs."""

    def __init__(self):
        super().__init__("BIGSERIAL")

    @property
    def python_type(self) -> Type[int]:
        return int

    def extends_column(self, column):
        column._has_default = True
        column._default_expr = None


class Numeric(PostgresType):
    """PostgreSQL NUMERIC type for precise decimal numbers."""

    def __init__(self, precision: Optional[int] = None, scale: Optional[int] = None):
        type_name = "NUMERIC"
        if precision is not None:
            if scale is not None:
                type_name = f"NUMERIC({precision}, {scale})"
            else:
                type_name = f"NUMERIC({precision})"
        super().__init__(type_name)
        self.precision = precision
        self.scale = scale

    @property
    def python_type(self) -> Type[float]:
        return float

    def extends_column(self, column):
        pass
