from typing import (
    Protocol,
    Any,
    AsyncContextManager,
    Callable,
    AsyncIterator,
    runtime_checkable,
)
from contextlib import asynccontextmanager


@runtime_checkable
class Connection(Protocol):
    """Protocol defining the expected interface for database connections."""

    def cursor(self) -> Any:
        """Return a cursor for executing SQL queries."""
        ...

    def execute(self, query: str, params: Any = None) -> Any:
        """Execute a SQL query directly on the connection."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...


@runtime_checkable
class AsyncConnection(Protocol):
    """Protocol defining the expected interface for async database connections."""

    async def cursor(self) -> AsyncContextManager[Any]:
        """Return an async cursor for executing SQL queries."""
        ...

    async def execute(self, query: str, params: Any = None) -> Any:
        """Execute a SQL query directly on the connection."""
        ...

    async def close(self) -> None:
        """Close the connection."""
        ...


@runtime_checkable
class ConnectionProvider(Protocol):
    """Protocol for objects that can provide database connections."""

    def __call__(self) -> Connection:
        """Return a synchronous database connection."""
        ...

    def get_async_connection(self) -> AsyncContextManager[AsyncConnection]:
        """Return an asynchronous database connection."""
        ...


class FunctionConnectionProvider:
    """Adapter to make a connection-providing function conform to ConnectionProvider protocol."""

    def __init__(self, connection_fn: Callable[[], Connection]):
        self.connection_fn = connection_fn

    def __call__(self) -> Connection:
        """Return a synchronous database connection."""
        return self.connection_fn()

    @asynccontextmanager
    async def get_async_connection(self) -> AsyncIterator[AsyncConnection]:
        """
        Fallback implementation that raises an error if async connections aren't supported.
        """
        try:
            raise NotImplementedError(
                "This connection provider does not support async connections"
            )
            yield None  # type: ignore just for typecheck, but should never be reached
        except NotImplementedError:
            raise
