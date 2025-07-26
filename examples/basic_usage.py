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
)
from catalyst_orm.postgres.tables import _Column
import uuid as py_uuid
from datetime import datetime
from typing import NamedTuple, List, Any


# define a table
class Users(PgTable):
    __tablename__ = "users"

    id: _Column[py_uuid.UUID] = UUID().primary_key()
    name: _Column[str] = Text()
    email: _Column[str] = Text().unique()
    age: _Column[int] = Integer().not_null()
    created_at: _Column[datetime] = Timestamp().default_sql("now()")


# mock connection
class MockCursor:
    def __init__(self):
        self._full_row = (
            py_uuid.uuid4(),
            "John Doe",
            "john.doe@email.com",
            30,
            datetime.now(),
        )
        self._all_cols_desc = [
            ("id",),
            ("name",),
            ("email",),
            ("age",),
            ("created_at",),
        ]
        self.description = []
        self._fetchall_result = []

    def fetchall(self):
        return self._fetchall_result

    def fetchone(self):
        if not self._fetchall_result:
            return None
        return self._fetchall_result[0]

    def close(self):
        pass

    def execute(self, query, params):
        import re

        if "INSERT" in query:
            self.description = [("id",)]
            self._fetchall_result = [(self._full_row[0],)]  # return id
            return

        match = re.search(r"SELECT (.*?) FROM", query)
        if not match:
            self.description = self._all_cols_desc
            self._fetchall_result = [self._full_row]
            return

        selected_columns_str = match.group(1)
        all_col_names = [d[0] for d in self._all_cols_desc]

        if (
            selected_columns_str == "*"
            or "users.id, users.name, users.email, users.age, users.created_at"
            in selected_columns_str
        ):
            selected_names = all_col_names
        else:
            selected_names = [
                c.strip().split(".")[-1] for c in selected_columns_str.split(",")
            ]

        try:
            indices = [all_col_names.index(name) for name in selected_names]
        except ValueError:
            self.description = self._all_cols_desc
            self._fetchall_result = [self._full_row]
            return

        self.description = [(name,) for name in selected_names]
        self._fetchall_result = [tuple(self._full_row[i] for i in indices)]


class MockConnection:
    def cursor(self):
        return MockCursor()

    def execute(self, query: str, params: Any = None):
        pass

    def close(self):
        pass


def mock_connection_provider():
    return MockConnection()


db = Database(mock_connection_provider)

users = Users()

query = (
    db.select(users.id, users.name, users.email)
    .from_(users)
    .where(
        and_(
            eq(users.email, "example@email.com"),
            gt(users.age, 25),
        )
    )
)

print(f"query.sql_with_params(): {query.sql_with_params()}")

results = query.execute()

for user in results:
    user_id, user_name, user_email = user
    print(f"User: id={user_id}, name={user_name}, email={user_email}")


class UserTuple(NamedTuple):
    id: py_uuid.UUID
    name: str
    email: str
    age: int
    created_at: datetime


table_query = db.select(Users).from_(users).where(eq(users.age, 30)).map_to(UserTuple)

user_tuples = table_query.execute()

for user in user_tuples:
    print(
        f"User: id={user.id}, name={user.name}, email={user.email}, age={user.age}, created_at={user.created_at}"
    )

insert_query = (
    db.insert(users)
    .values(
        name="test",
        email="test@email.com",
        age=12,
    )
    .returning(users.id)
)

user_id: py_uuid.UUID = insert_query.execute()
print(f"new user has been created with id={user_id}")
