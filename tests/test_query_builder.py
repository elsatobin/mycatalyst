import pytest
import uuid
from datetime import datetime
from typing import NamedTuple, List

from catalyst_orm import (
    Database,
    PgTable,
    Text,
    Integer,
    UUID,
    Timestamp,
    eq,
    gt,
    and_,
    or_,
)
from catalyst_orm.postgres.tables import _Column
import uuid as py_uuid
from datetime import datetime
from typing import NamedTuple, List, Tuple


# Mocking infrastructure
class MockCursor:
    def __init__(self, description=None, fetchall_result=None, fetchone_result=None):
        self.description = description
        self._fetchall_result = fetchall_result
        self._fetchone_result = fetchone_result
        self.query = None
        self.params = None

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return self._fetchall_result

    def fetchone(self):
        return self._fetchone_result

    def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass

    def execute(self, query, params):
        self._cursor.execute(query, params)

    async def get_async_connection(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


# Table definition for testing
class Users(PgTable):
    __tablename__ = "users"

    id: _Column[py_uuid.UUID] = UUID().primary_key()
    name: _Column[str] = Text()
    email: _Column[str] = Text().unique()
    age: _Column[int] = Integer().not_null()
    created_at: _Column[datetime] = Timestamp().default_sql("now()")


users = Users()


@pytest.fixture
def db_and_cursor():
    cursor = MockCursor()
    connection = MockConnection(cursor)
    db = Database(lambda: connection)
    return db, cursor


def test_select_one_column(db):
    query = db.select(users.id).from_(users)
    sql, params = query.build()
    assert sql == "SELECT users.id FROM users"
    assert params == []


def test_select_multiple_columns(db):
    query = db.select(users.id, users.name).from_(users)
    sql, params = query.build()
    assert sql == "SELECT users.id, users.name FROM users"
    assert params == []


def test_select_with_where(db):
    query = db.select(users.id).from_(users).where(eq(users.age, 30))
    sql, params = query.build()
    assert sql == "SELECT users.id FROM users WHERE (users.age = %s)"
    assert params == [30]


def test_select_with_and_where(db):
    query = (
        db.select(users.id)
        .from_(users)
        .where(
            and_(
                eq(users.email, "test@example.com"),
                gt(users.age, 30),
            )
        )
    )
    sql, params = query.build()
    assert (
        sql
        == "SELECT users.id FROM users WHERE ((users.email = %s) AND (users.age > %s))"
    )
    assert params == ["test@example.com", 30]


def test_select_with_or_where(db):
    query = (
        db.select(users.id)
        .from_(users)
        .where(
            or_(
                eq(users.email, "test@example.com"),
                gt(users.age, 30),
            )
        )
    )
    sql, params = query.build()
    assert (
        sql
        == "SELECT users.id FROM users WHERE ((users.email = %s) OR (users.age > %s))"
    )
    assert params == ["test@example.com", 30]


def test_insert(db):
    query = db.insert(users).values(
        name="John Doe", email="john.doe@example.com", age=30
    )
    sql, params = query.build()
    assert sql == "INSERT INTO users (name, email, age) VALUES (%s, %s, %s)"
    assert params == ["John Doe", "john.doe@example.com", 30]


def test_insert_returning(db):
    query = (
        db.insert(users)
        .values(name="John Doe", email="john.doe@example.com", age=30)
        .returning(users.id)
    )
    sql, params = query.build()
    assert (
        sql
        == "INSERT INTO users (name, email, age) VALUES (%s, %s, %s) RETURNING users.id"
    )
    assert params == ["John Doe", "john.doe@example.com", 30]


def test_update(db):
    query = db.update(users).set(age=31).where(eq(users.email, "john.doe@example.com"))
    sql, params = query.build()
    assert sql == "UPDATE users SET age = %s WHERE (users.email = %s)"
    assert params == [31, "john.doe@example.com"]


def test_delete(db):
    query = db.delete(users).where(eq(users.email, "john.doe@example.com"))
    sql, params = query.build()
    assert sql == "DELETE FROM users WHERE (users.email = %s)"
    assert params == ["john.doe@example.com"]


def test_map_to(db):
    class UserTuple(NamedTuple):
        id: py_uuid.UUID
        name: str

    query = db.select(users.id, users.name).from_(users).map_to(UserTuple)
    assert query._model_class == UserTuple
    results = query.execute()
    assert isinstance(results[0], UserTuple)
    assert results[0].name == "John Doe"


def test_async_execution(db):
    import asyncio

    async def run():
        query = db.select(users.id).from_(users)
        results = await query
        assert results is not None

    asyncio.run(run())
