import pytest
from catalyst_orm import Database
from catalyst_orm.connection_interface import Connection
from typing import Any, List, Tuple, AsyncIterator
import uuid as py_uuid
from datetime import datetime
from contextlib import asynccontextmanager


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
        self.description: List[Tuple[str, ...]] = []
        self._fetchall_result: List[Any] = []
        self.rowcount = 0

    def fetchall(self):
        return self._fetchall_result

    def fetchone(self):
        if not self._fetchall_result:
            return None
        return self._fetchall_result[0]

    def close(self):
        pass

    def execute(self, query, params=None):
        import re

        self.rowcount = 0
        if "INSERT" in query:
            self.description = [("id",)]
            self._fetchall_result = [(self._full_row[0],)]
            self.rowcount = 1
            return

        if "UPDATE" in query or "DELETE" in query:
            self.rowcount = 1
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
        self.rowcount = len(self._fetchall_result)


class MockConnection:
    def cursor(self):
        return MockCursor()

    def execute(self, query: str, params: Any = None):
        pass

    def close(self):
        pass


class MockAsyncCursor(MockCursor):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def execute(self, query, params=None):
        super().execute(query, params)


class MockAsyncConnection(MockConnection):
    async def cursor(self):
        return MockAsyncCursor()

    async def execute(self, query: str, params: Any = None):
        pass


class MockConnectionProvider:
    def __call__(self) -> Connection:
        return MockConnection()

    @asynccontextmanager
    async def get_async_connection(self) -> AsyncIterator[MockAsyncConnection]:
        yield MockAsyncConnection()


@pytest.fixture
def db():
    return Database(MockConnectionProvider())
