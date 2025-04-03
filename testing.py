from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Protocol, cast
from catalyst_orm.connection_interface import AsyncConnection, ConnectionProvider
from catalyst_orm.postgres.column_types import UUID, Integer, Text, Timestamp
from catalyst_orm.postgres.tables import PgTable, column, _Column
from catalyst_orm.conditions import eq, gt
from catalyst_orm.query_builder import Database
import psycopg


class UUIDColumn(Protocol):
    def uuid(self) -> "_Column[Any]": ...


class TimestampColumn(Protocol):
    def now(self) -> "_Column[Any]": ...


class Users(PgTable):
    __tablename__ = "users"

    id = cast(UUIDColumn, column(UUID())).uuid().primary()
    name = column(Text())
    email = column(Text()).unique()
    age = column(Integer())
    created_at = cast(TimestampColumn, column(Timestamp(with_timezone=True))).now()


users = Users()


async def get_connection():
    try:
        aconn = await psycopg.AsyncConnection.connect(
            dbname="your_db_name",
            user="your_username",
            password="your_password",
            host="localhost",
        )
        return aconn
    except Exception as e:
        print(f"database connection error: {str(e)}")
        raise


class AsyncPgConnectionProvider(ConnectionProvider):
    def __init__(self, connection_fn):
        self.connection_fn = connection_fn

    def __call__(self):
        raise NotImplementedError("synchronous connections are not supported")

    @asynccontextmanager
    async def get_async_connection(self) -> AsyncIterator[AsyncConnection]:
        conn = await self.connection_fn()
        try:
            yield conn
        finally:
            await conn.close()


db = Database(AsyncPgConnectionProvider(get_connection))

query = (
    db.select(users.id, users.name, users.email)
    .from_(users)
    .where(eq(users.email, "example@email.com"))
    .and_where(gt(users.age, 25))
)

sql = query.sql_with_params()
print(f"sql: {sql}")
