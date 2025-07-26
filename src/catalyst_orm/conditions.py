from typing import Any, Tuple, List, TypeVar, Union
from abc import ABC, abstractmethod
from .postgres.tables import _Column

T = TypeVar("T")


class Condition(ABC):
    @abstractmethod
    def build(self) -> Tuple[str, List[Any]]:
        pass


class Junction(Condition):
    def __init__(self, operator: str, *conditions: Condition):
        if len(conditions) < 1:
            raise ValueError(f"{operator} requires at least one condition")
        self.operator = operator
        self.conditions = conditions

    def build(self) -> Tuple[str, List[Any]]:
        all_sql: List[str] = []
        all_params: List[Any] = []
        for condition in self.conditions:
            sql, params = condition.build()
            all_sql.append(sql)
            all_params.extend(params)

        if not all_sql:
            return "", []

        if len(all_sql) == 1:
            return all_sql[0], all_params

        joined_parts = []
        for sql in all_sql:
            if sql.startswith("(") and sql.endswith(")"):
                joined_parts.append(sql)
            else:
                joined_parts.append(f"({sql})")

        joined_sql = f" {self.operator} ".join(joined_parts)
        return f"({joined_sql})", all_params


class BinaryCondition(Condition):
    def __init__(self, operator: str, left: Any, right: Any):
        self.operator = operator
        self.left = left
        self.right = right

        if (
            isinstance(left, _Column)
            and isinstance(right, _Column)
            and left.sql_type is not None
            and right.sql_type is not None
        ):
            left_type = left.sql_type.python_type
            right_type = right.sql_type.python_type
            if left_type != right_type:
                raise TypeError(
                    f"Type mismatch in condition: {left_type} vs {right_type}"
                )
        elif isinstance(left, _Column) and left.sql_type is not None:
            expected_type = left.sql_type.python_type
            if right is not None and not isinstance(right, expected_type):
                raise TypeError(
                    f"Expected {expected_type} for {left}, got {type(right)}"
                )
        elif isinstance(right, _Column) and right.sql_type is not None:
            expected_type = right.sql_type.python_type
            if left is not None and not isinstance(left, expected_type):
                raise TypeError(
                    f"Expected {expected_type} for {right}, got {type(left)}"
                )

    def build(self) -> Tuple[str, List[Any]]:
        params: List[Any] = []
        if isinstance(self.left, _Column):
            if self.left.table is None:
                raise ValueError("Column table cannot be None when building condition")
            left_part = f"{self.left.table._alias}.{self.left.name}"
        else:
            left_part = "%s"
            params.append(self.left)

        if isinstance(self.right, _Column):
            if self.right.table is None:
                raise ValueError("Column table cannot be None when building condition")
            right_part = f"{self.right.table._alias}.{self.right.name}"
        else:
            right_part = "%s"
            params.append(self.right)

        return f"{left_part} {self.operator} {right_part}", params


def and_(*conditions: Condition) -> Condition:
    if not conditions:
        raise ValueError("and_() requires at least one condition")
    if len(conditions) == 1:
        return conditions[0]
    return Junction("AND", *conditions)


def or_(*conditions: Condition) -> Condition:
    if not conditions:
        raise ValueError("or_() requires at least one condition")
    if len(conditions) == 1:
        return conditions[0]
    return Junction("OR", *conditions)


def eq(left: Union[_Column[T], T], right: Union[_Column[T], T]) -> BinaryCondition:
    return BinaryCondition("=", left, right)


def ne(left: Union[_Column[T], T], right: Union[_Column[T], T]) -> BinaryCondition:
    return BinaryCondition("!=", left, right)


def gt(left: Union[_Column[T], T], right: Union[_Column[T], T]) -> BinaryCondition:
    return BinaryCondition(">", left, right)


def lt(left: Union[_Column[T], T], right: Union[_Column[T], T]) -> BinaryCondition:
    return BinaryCondition("<", left, right)


def gte(left: Union[_Column[T], T], right: Union[_Column[T], T]) -> BinaryCondition:
    return BinaryCondition(">=", left, right)


def lte(left: Union[_Column[T], T], right: Union[_Column[T], T]) -> BinaryCondition:
    return BinaryCondition("<=", left, right)


class RawSQL(Condition):
    def __init__(self, sql: str, *params: Any):
        self.sql = sql
        self.params = params

    def build(self) -> Tuple[str, List[Any]]:
        return self.sql, list(self.params)


class InCondition(Condition):
    def __init__(self, column: _Column[T], values: List[T]):
        self.column = column
        self.values = values

        if column.sql_type is not None:
            expected_type = column.sql_type.python_type
            for val in values:
                if val is not None and not isinstance(val, expected_type):
                    raise TypeError(
                        f"Expected list of {expected_type} for IN condition, got {type(val)}"
                    )

    def build(self) -> Tuple[str, List[Any]]:
        if not self.values:
            return ("FALSE", [])

        if self.column.table is None:
            raise ValueError("Column table cannot be None when building IN condition")

        placeholders = ", ".join(["%s"] * len(self.values))
        return (
            f"{self.column.table._alias}.{self.column.name} IN ({placeholders})",
            list(self.values),
        )


def in_(column: _Column[T], values: List[T]) -> InCondition:
    return InCondition(column, values)


class NotInCondition(Condition):
    def __init__(self, column: _Column[T], values: List[T]):
        self.column = column
        self.values = values

        if column.sql_type is not None:
            expected_type = column.sql_type.python_type
            for val in values:
                if val is not None and not isinstance(val, expected_type):
                    raise TypeError(
                        f"Expected list of {expected_type} for NOT IN condition, got {type(val)}"
                    )

    def build(self) -> Tuple[str, List[Any]]:
        if not self.values:
            return ("TRUE", [])

        if self.column.table is None:
            raise ValueError(
                "Column table cannot be None when building NOT IN condition"
            )

        placeholders = ", ".join(["%s"] * len(self.values))
        return (
            f"{self.column.table._alias}.{self.column.name} NOT IN ({placeholders})",
            list(self.values),
        )


def not_in(column: _Column[T], values: List[T]) -> NotInCondition:
    return NotInCondition(column, values)


class LikeCondition(Condition):
    def __init__(self, column: _Column[str], pattern: str):
        self.column = column
        self.pattern = pattern

        if column.sql_type is not None and column.sql_type.python_type is not str:
            raise TypeError(
                f"LIKE can only be used with text columns, got {column.sql_type.python_type}"
            )

    def build(self) -> Tuple[str, List[Any]]:
        if self.column.table is None:
            raise ValueError("Column table cannot be None when building LIKE condition")

        return f"{self.column.table._alias}.{self.column.name} LIKE %s", [self.pattern]


def like(column: _Column[str], pattern: str) -> LikeCondition:
    return LikeCondition(column, pattern)


class NotLikeCondition(Condition):
    def __init__(self, column: _Column[str], pattern: str):
        self.column = column
        self.pattern = pattern

        if column.sql_type is not None and column.sql_type.python_type is not str:
            raise TypeError(
                f"NOT LIKE can only be used with text columns, got {column.sql_type.python_type}"
            )

    def build(self) -> Tuple[str, List[Any]]:
        if self.column.table is None:
            raise ValueError(
                "Column table cannot be None when building NOT LIKE condition"
            )

        return f"{self.column.table._alias}.{self.column.name} NOT LIKE %s", [
            self.pattern
        ]


def not_like(column: _Column[str], pattern: str) -> NotLikeCondition:
    return NotLikeCondition(column, pattern)


class IsNullCondition(Condition):
    def __init__(self, column: _Column[Any]):
        self.column = column

    def build(self) -> Tuple[str, List[Any]]:
        if self.column.table is None:
            raise ValueError(
                "Column table cannot be None when building IS NULL condition"
            )

        return (f"{self.column.table._alias}.{self.column.name} IS NULL", [])


def is_null(column: _Column[Any]) -> IsNullCondition:
    return IsNullCondition(column)


class IsNotNullCondition(Condition):
    def __init__(self, column: _Column[Any]):
        self.column = column

    def build(self) -> Tuple[str, List[Any]]:
        if self.column.table is None:
            raise ValueError(
                "Column table cannot be None when building IS NOT NULL condition"
            )

        return (f"{self.column.table._alias}.{self.column.name} IS NOT NULL", [])


def is_not_null(column: _Column[Any]) -> IsNotNullCondition:
    return IsNotNullCondition(column)
