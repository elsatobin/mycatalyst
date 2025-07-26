from catalyst_orm import (
    PgTable,
    Text,
    Integer,
    UUID,
    Timestamp,
    eq,
    ne,
    gt,
    lt,
    gte,
    lte,
    and_,
    or_,
    in_,
    not_in,
    like,
    not_like,
    is_null,
    is_not_null,
)
from catalyst_orm.postgres.tables import _Column
import uuid as py_uuid
from datetime import datetime


class Users(PgTable):
    __tablename__ = "users"
    id: _Column[py_uuid.UUID] = UUID().primary_key()
    name: _Column[str] = Text()
    email: _Column[str] = Text().unique()
    age: _Column[int] = Integer().not_null()
    created_at: _Column[datetime] = Timestamp().default_sql("now()")


users = Users()


def test_eq():
    condition = eq(users.age, 30)
    sql, params = condition.build()
    assert sql == "users.age = %s"
    assert params == [30]


def test_ne():
    condition = ne(users.age, 30)
    sql, params = condition.build()
    assert sql == "users.age != %s"
    assert params == [30]


def test_gt():
    condition = gt(users.age, 30)
    sql, params = condition.build()
    assert sql == "users.age > %s"
    assert params == [30]


def test_lt():
    condition = lt(users.age, 30)
    sql, params = condition.build()
    assert sql == "users.age < %s"
    assert params == [30]


def test_gte():
    condition = gte(users.age, 30)
    sql, params = condition.build()
    assert sql == "users.age >= %s"
    assert params == [30]


def test_lte():
    condition = lte(users.age, 30)
    sql, params = condition.build()
    assert sql == "users.age <= %s"
    assert params == [30]


def test_and():
    condition = and_(eq(users.name, "John"), gt(users.age, 30))
    sql, params = condition.build()
    assert sql == "((users.name = %s) AND (users.age > %s))"
    assert params == ["John", 30]


def test_or():
    condition = or_(eq(users.name, "John"), gt(users.age, 30))
    sql, params = condition.build()
    assert sql == "((users.name = %s) OR (users.age > %s))"
    assert params == ["John", 30]


def test_nested_and_or():
    condition = and_(
        eq(users.name, "John"),
        or_(
            gt(users.age, 30),
            lt(users.age, 20),
        ),
    )
    sql, params = condition.build()
    assert sql == "((users.name = %s) AND ((users.age > %s) OR (users.age < %s)))"
    assert params == ["John", 30, 20]


def test_in():
    condition = in_(users.age, [20, 30, 40])
    sql, params = condition.build()
    assert sql == "users.age IN (%s, %s, %s)"
    assert params == [20, 30, 40]


def test_not_in():
    condition = not_in(users.age, [20, 30, 40])
    sql, params = condition.build()
    assert sql == "users.age NOT IN (%s, %s, %s)"
    assert params == [20, 30, 40]


def test_like():
    condition = like(users.name, "J%")
    sql, params = condition.build()
    assert sql == "users.name LIKE %s"
    assert params == ["J%"]


def test_not_like():
    condition = not_like(users.name, "J%")
    sql, params = condition.build()
    assert sql == "users.name NOT LIKE %s"
    assert params == ["J%"]


def test_is_null():
    condition = is_null(users.email)
    sql, params = condition.build()
    assert sql == "users.email IS NULL"
    assert params == []


def test_is_not_null():
    condition = is_not_null(users.email)
    sql, params = condition.build()
    assert sql == "users.email IS NOT NULL"
    assert params == []
