import asyncio
import uuid
from abc import abstractmethod
from contextlib import asynccontextmanager
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from catalyst_orm.connection_interface import (
    Connection,
    ConnectionProvider,
    FunctionConnectionProvider,
)

from .conditions import Condition, and_
from .postgres.tables import PgTable, _Column, TableT

T = TypeVar("T")
R = TypeVar("R")
ModelT = TypeVar("ModelT")
T_Row = TypeVar("T_Row")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")


class QueryResult(Generic[R]):
    def __init__(self, query: "Query[R]"):
        self.query = query

    def __await__(self):
        async def execute_async():
            result = await self.query._execute_async()
            return result

        return execute_async().__await__()


class Query(Generic[R]):
    def __init__(
        self, connection_provider: Union[ConnectionProvider, Callable[[], Connection]]
    ):
        if callable(connection_provider) and not hasattr(
            connection_provider, "get_async_connection"
        ):
            self.connection_provider = FunctionConnectionProvider(connection_provider)
        else:
            self.connection_provider = cast(ConnectionProvider, connection_provider)
        self._conditions: List[Condition] = []
        self._model_class: Optional[Type[Any]] = None

    def where(self, *conditions: Condition) -> "Query[R]":
        if not conditions:
            return self
        if len(conditions) == 1:
            self._conditions.append(conditions[0])
        else:
            self._conditions.append(and_(*conditions))
        return self

    def build_where_clause(self) -> Tuple[str, List[Any]]:
        """
        Build the WHERE clause for the query.

        Returns:
            Tuple[str, List[Any]]: SQL WHERE clause string and parameters
        """
        if not self._conditions:
            return "", []

        # there will always be a single root condition
        condition = self._conditions[0]

        sql, params = condition.build()

        if sql:
            if not sql.startswith("("):
                sql = f"({sql})"
            return f" WHERE {sql}", params
        return "", params

    def map_to(self, model_class: Type[ModelT]) -> "Query[List[ModelT]]":
        self._model_class = model_class
        return cast(Query[List[ModelT]], self)

    @abstractmethod
    def build(self) -> Tuple[str, List[Any]]:
        pass

    def execute(self) -> R:
        query, params = self.build()

        conn = None
        cursor = None
        try:
            conn = self.connection_provider()
            if conn is None:
                raise ValueError("Failed to establish database connection")

            cursor = conn.cursor()
            if cursor is None:
                raise ValueError("Failed to create database cursor")

            cursor.execute(query, params)
            return self._process_results(cursor)

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None and not hasattr(conn, "_is_from_pool"):
                conn.close()

    async def _execute_async(self) -> R:
        query, params = self.build()

        if hasattr(self.connection_provider, "get_async_connection"):
            async with self.connection_provider.get_async_connection() as conn:
                cursor = await conn.cursor()
                async with cursor as cursor:
                    await cursor.execute(query, params)
                    return await self._process_async_results(cursor)
        else:
            return await asyncio.to_thread(self.execute)

    def _process_results(self, cursor: Any) -> R:
        raise NotImplementedError("_process_results must be implemented by subclasses")

    async def _process_async_results(self, cursor: Any) -> R:
        return self._process_results(cursor)

    def __await__(self):
        return QueryResult(self).__await__()

    def sql(self) -> str:
        query_sql, _ = self.build()
        return query_sql

    def sql_with_params(self) -> str:
        query_sql, params = self.build()

        import re

        query_sql = re.sub(
            r"([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)", r"\1.\3", query_sql
        )

        for param in params:
            if param is None:
                param_str = "NULL"
            elif isinstance(param, (str, uuid.UUID)):
                param_str = f"'{param}'"
            else:
                param_str = str(param)
            query_sql = query_sql.replace("%s", param_str, 1)

        return query_sql


class SelectQuery(Query[List[T_Row]], Generic[T_Row]):
    def __init__(self, connection_provider: Callable):
        super().__init__(connection_provider)
        self._tables: List[PgTable] = []
        self._columns: List[Union[str, _Column[Any]]] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._order_by: List[Tuple[_Column[Any], str]] = []
        self._group_by: List[_Column[Any]] = []
        self._join_clauses: List[str] = []
        self._join_params: List[Any] = []
        self._with_relations: List[str] = []

    def select(self, *columns: Union[str, _Column[Any]]) -> "SelectQuery[T_Row]":
        self._columns = list(columns) if columns else ["*"]
        return self

    def from_(self, table: PgTable) -> "SelectQuery[T_Row]":
        self._tables.append(table)
        return self

    def limit(self, limit: int) -> "SelectQuery[T_Row]":
        if limit < 0:
            raise ValueError("LIMIT cannot be negative")
        self._limit = limit
        return self

    def offset(self, offset: int) -> "SelectQuery[T_Row]":
        if offset < 0:
            raise ValueError("OFFSET cannot be negative")
        self._offset = offset
        return self

    def order_by(
        self, column: _Column[Any], direction: str = "ASC"
    ) -> "SelectQuery[T_Row]":
        direction = direction.upper()
        if direction not in ("ASC", "DESC"):
            raise ValueError("Direction must be either 'ASC' or 'DESC'")
        self._order_by.append((column, direction))
        return self

    def group_by(self, column: _Column[Any]) -> "SelectQuery[T_Row]":
        self._group_by.append(column)
        return self

    def join(self, table: PgTable, condition: Condition) -> "SelectQuery[T_Row]":
        sql, params = condition.build()
        self._join_clauses.append(f"INNER JOIN {table._alias} ON {sql}")
        self._join_params.extend(params)
        return self

    def left_join(self, table: PgTable, condition: Condition) -> "SelectQuery[T_Row]":
        sql, params = condition.build()
        self._join_clauses.append(f"LEFT JOIN {table._alias} ON {sql}")
        self._join_params.extend(params)
        return self

    def right_join(self, table: PgTable, condition: Condition) -> "SelectQuery[T_Row]":
        sql, params = condition.build()
        self._join_clauses.append(f"RIGHT JOIN {table._alias} ON {sql}")
        self._join_params.extend(params)
        return self

    def with_(self, *relations) -> "SelectQuery[T_Row]":
        for relation in relations:
            if isinstance(relation, str):
                self._with_relations.append(relation)
            else:
                self._with_relations.append(relation.relation_name)
        return self

    def load_relation(self, relation) -> "SelectQuery[T_Row]":
        if not self._tables:
            raise ValueError("No source table specified for relation query")

        source_table = self._tables[0]

        if isinstance(relation, str):
            relation_name = relation
            relation_obj = source_table._relations.get(relation_name)
            if relation_obj is None:
                raise ValueError(
                    f"Relation '{relation_name}' not found on table {source_table._alias}"
                )
        else:
            relation_obj = relation.relation

        if relation_obj.type == "many_to_many":
            self.join(
                relation_obj.through_table, relation_obj.get_first_join_condition()
            )
            self.join(
                relation_obj.target_table, relation_obj.get_second_join_condition()
            )
        else:
            target_table = relation_obj.target_table
            condition = relation_obj.get_join_condition()
            self.join(target_table, condition)

        return self

    def map_to(self, model_class: Type[ModelT]) -> "SelectQuery[ModelT]":
        self._model_class = model_class
        return cast("SelectQuery[ModelT]", self)

    def build(self) -> Tuple[str, List[Any]]:
        if not self._tables:
            raise ValueError("no table specified for the query")

        def get_clean_table_name(table_obj):
            if hasattr(table_obj, "__tablename__"):
                return table_obj.__tablename__

            if hasattr(table_obj, "name"):
                table_name = str(table_obj.name)
                if "." in table_name:
                    return table_name.split(".")[0]
                return table_name

            # last resort: convert to string and extract first part
            table_str = str(table_obj)
            if "." in table_str:
                return table_str.split(".")[0]
            return table_str

        # handle columns in select clause
        select_parts = []
        for col in self._columns:
            if isinstance(col, _Column):
                if col.table is None:
                    raise ValueError(
                        "column table cannot be none when building select query"
                    )

                # get clean table name for the column
                table_name = get_clean_table_name(col.table)
                select_parts.append(f"{table_name}.{col.name}")
            else:
                select_parts.append(str(col))

        select_clause = f"SELECT {', '.join(select_parts)}"

        # handle tables in from clause
        from_parts = []
        for table in self._tables:
            if isinstance(table, str):
                from_parts.append(table)
            else:
                # extract clean table name
                from_parts.append(get_clean_table_name(table))

        from_clause = f" FROM {', '.join(from_parts)}"

        # join clauses likely need cleaning too, but we'll leave that for now
        join_clause = " " + " ".join(self._join_clauses) if self._join_clauses else ""

        where_clause, where_params = self.build_where_clause()

        # fix group by clause to use clean table names
        group_by_clause = ""
        if self._group_by:
            group_by_parts = []
            for col in self._group_by:
                if col.table is None:
                    raise ValueError(
                        "column table cannot be none when building group by clause"
                    )
                # get clean table name
                table_name = get_clean_table_name(col.table)
                group_by_parts.append(f"{table_name}.{col.name}")
            group_by_clause = f" GROUP BY {', '.join(group_by_parts)}"

        # fix order by clause to use clean table names
        order_by_clause = ""
        if self._order_by:
            order_by_parts = []
            for col, direction in self._order_by:
                if col.table is None:
                    raise ValueError(
                        "column table cannot be none when building order by clause"
                    )
                # get clean table name
                table_name = get_clean_table_name(col.table)
                order_by_parts.append(f"{table_name}.{col.name} {direction}")
            order_by_clause = f" ORDER BY {', '.join(order_by_parts)}"

        limit_clause = f" LIMIT {self._limit}" if self._limit is not None else ""
        offset_clause = f" OFFSET {self._offset}" if self._offset is not None else ""

        query = (
            select_clause
            + from_clause
            + join_clause
            + where_clause
            + group_by_clause
            + order_by_clause
            + limit_clause
            + offset_clause
        )

        all_params = self._join_params + where_params

        return query, all_params

    def _process_results(self, cursor: Any) -> List[T_Row]:
        results = cursor.fetchall()

        if not results:
            return []

        column_names = (
            [desc[0] for desc in cursor.description] if cursor.description else []
        )

        if self._model_class is not None:
            rows = []
            for row in results:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(column_names):
                        row_dict[column_names[i]] = value

                try:
                    model_instance = self._model_class(**row_dict)
                    rows.append(model_instance)
                except Exception as e:
                    raise ValueError(
                        f"Failed to map row to {self._model_class.__name__}: {e}"
                    )
            return rows  # type: ignore[return-value]
        else:
            if cursor.description and len(cursor.description) == 1:
                return [row[0] for row in results]  # type: ignore[return-value]
            return results  # type: ignore[return-value]


class InsertQuery(Query[Any]):
    def __init__(self, connection_provider: Callable, table: PgTable):
        super().__init__(connection_provider)
        self._table = table
        self._values: Dict[str, Any] = {}
        self._returning: List[Union[str, _Column[Any]]] = []

    def values(self, **kwargs: Any) -> "InsertQuery":
        self._values.update(kwargs)
        return self

    def returning(self, *columns: Union[str, _Column[Any]]) -> "InsertQuery":
        self._returning.extend(columns)
        return self

    def build(self) -> Tuple[str, List[Any]]:
        if not self._values:
            raise ValueError("No values provided for insert query")

        columns = list(self._values.keys())
        placeholders = ["%s"] * len(columns)
        values = [self._values[col] for col in columns]

        query = f"INSERT INTO {self._table._alias} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        if self._returning:
            returning_parts = []
            for col in self._returning:
                if isinstance(col, _Column):
                    if col.table is None:
                        raise ValueError(
                            "Column table cannot be None when building RETURNING clause"
                        )
                    returning_parts.append(f"{col.table._alias}.{col.name}")
                else:
                    returning_parts.append(str(col))
            query += f" RETURNING {', '.join(returning_parts)}"

        return query, values

    def _process_results(self, cursor: Any) -> Any:
        if self._returning:
            result = cursor.fetchone()
            if result is None:
                return None

            if self._model_class is not None:
                column_names = [desc[0] for desc in cursor.description]
                row_dict = {column_names[i]: value for i, value in enumerate(result)}
                try:
                    return self._model_class(**row_dict)
                except Exception as e:
                    raise ValueError(
                        f"Failed to map row to {self._model_class.__name__}: {e}"
                    )

            if len(result) == 1:
                return result[0]

            return result

        return cursor.rowcount


class UpdateQuery(Query[int]):
    def __init__(self, connection_provider: Callable, table: PgTable):
        super().__init__(connection_provider)
        self._table = table
        self._values: Dict[str, Any] = {}
        self._returning: List[Union[str, _Column[Any]]] = []

    def set(self, **kwargs: Any) -> "UpdateQuery":
        self._values.update(kwargs)
        return self

    def returning(self, *columns: Union[str, _Column[Any]]) -> "UpdateQuery":
        self._returning.extend(columns)
        return self

    def build(self) -> Tuple[str, List[Any]]:
        if not self._values:
            raise ValueError("No values provided for update query")

        set_parts = [f"{col} = %s" for col in self._values.keys()]
        values = list(self._values.values())

        query = f"UPDATE {self._table._alias} SET {', '.join(set_parts)}"

        where_clause, where_params = self.build_where_clause()
        query += where_clause

        if self._returning:
            returning_parts = []
            for col in self._returning:
                if isinstance(col, _Column):
                    if col.table is None:
                        raise ValueError(
                            "Column table cannot be None when building RETURNING clause"
                        )
                    returning_parts.append(f"{col.table._alias}.{col.name}")
                else:
                    returning_parts.append(str(col))
            query += f" RETURNING {', '.join(returning_parts)}"

        return query, values + where_params

    def _process_results(self, cursor: Any) -> Any:
        if self._returning:
            results = cursor.fetchall()

            if not results:
                return []

            column_names = [desc[0] for desc in cursor.description]

            rows = []
            for row in results:
                row_dict = {column_names[i]: value for i, value in enumerate(row)}

                if self._model_class is not None:
                    try:
                        model_instance = self._model_class(**row_dict)
                        rows.append(model_instance)
                    except Exception as e:
                        raise ValueError(
                            f"Failed to map row to {self._model_class.__name__}: {e}"
                        )
                else:
                    rows.append(row_dict)

            return rows if len(rows) > 1 else rows[0] if rows else None

        return cursor.rowcount


class DeleteQuery(Query[int]):
    def __init__(self, connection_provider: Callable, table: PgTable):
        super().__init__(connection_provider)
        self._table = table
        self._returning: List[Union[str, _Column[Any]]] = []

    def returning(self, *columns: Union[str, _Column[Any]]) -> "DeleteQuery":
        self._returning.extend(columns)
        return self

    def build(self) -> Tuple[str, List[Any]]:
        query = f"DELETE FROM {self._table._alias}"

        where_clause, where_params = self.build_where_clause()
        query += where_clause

        if self._returning:
            returning_parts = []
            for col in self._returning:
                if isinstance(col, _Column):
                    if col.table is None:
                        raise ValueError(
                            "Column table cannot be None when building RETURNING clause"
                        )
                    returning_parts.append(f"{col.table._alias}.{col.name}")
                else:
                    returning_parts.append(str(col))
            query += f" RETURNING {', '.join(returning_parts)}"

        return query, where_params

    def _process_results(self, cursor: Any) -> Any:
        if self._returning:
            results = cursor.fetchall()

            if not results:
                return []

            column_names = [desc[0] for desc in cursor.description]

            rows = []
            for row in results:
                row_dict = {column_names[i]: value for i, value in enumerate(row)}

                if self._model_class is not None:
                    try:
                        model_instance = self._model_class(**row_dict)
                        rows.append(model_instance)
                    except Exception as e:
                        raise ValueError(
                            f"Failed to map row to {self._model_class.__name__}: {e}"
                        )
                else:
                    rows.append(row_dict)

            return rows if len(rows) > 1 else rows[0] if rows else None

        return cursor.rowcount


class Database:
    def __init__(
        self, connection_provider: Union[ConnectionProvider, Callable[[], Connection]]
    ):
        if callable(connection_provider) and not hasattr(
            connection_provider, "get_async_connection"
        ):
            self.connection_provider = FunctionConnectionProvider(connection_provider)
        else:
            self.connection_provider = cast(ConnectionProvider, connection_provider)

    @overload
    def select(self, __c1: _Column[T1]) -> SelectQuery[T1]: ...

    @overload
    def select(
        self, __c1: _Column[T1], __c2: _Column[T2]
    ) -> SelectQuery[Tuple[T1, T2]]: ...

    @overload
    def select(
        self, __c1: _Column[T1], __c2: _Column[T2], __c3: _Column[T3]
    ) -> SelectQuery[Tuple[T1, T2, T3]]: ...

    @overload
    def select(
        self, __c1: _Column[T1], __c2: _Column[T2], __c3: _Column[T3], __c4: _Column[T4]
    ) -> SelectQuery[Tuple[T1, T2, T3, T4]]: ...

    @overload
    def select(
        self,
        __c1: _Column[T1],
        __c2: _Column[T2],
        __c3: _Column[T3],
        __c4: _Column[T4],
        __c5: _Column[T5],
    ) -> SelectQuery[Tuple[T1, T2, T3, T4, T5]]: ...

    @overload
    def select(self, __entity: Type[TableT]) -> SelectQuery[TableT]: ...

    def select(
        self, *columns: Union[str, _Column[Any], Type[PgTable]]
    ) -> SelectQuery[Any]:
        if columns and isinstance(columns[0], type) and issubclass(columns[0], PgTable):
            table_class = columns[0]
            instance = table_class()
            return (
                SelectQuery(self.connection_provider)
                .select(*instance.get_columns().keys())
                .from_(instance)
                .map_to(table_class)
            )
        clean_columns = [c for c in columns if isinstance(c, (str, _Column))]
        if len(clean_columns) != len(columns):
            raise TypeError(
                "Invalid argument for select. Cannot mix table classes with columns."
            )
        query = SelectQuery(self.connection_provider)
        return query.select(*clean_columns)

    def insert(self, table: PgTable) -> InsertQuery:
        return InsertQuery(self.connection_provider, table)

    def update(self, table: PgTable) -> UpdateQuery:
        return UpdateQuery(self.connection_provider, table)

    def delete(self, table: PgTable) -> DeleteQuery:
        return DeleteQuery(self.connection_provider, table)

    def query(self, table: PgTable) -> SelectQuery:
        query = SelectQuery(self.connection_provider)
        query.select().from_(table)
        return query

    def related(self, relation_ref) -> SelectQuery:
        query = SelectQuery(self.connection_provider)

        relation_obj = relation_ref.relation
        target_table = relation_obj.target_table

        if relation_obj.type == "belongs_to":
            query.select().from_(target_table)
            query.where(relation_obj.get_join_condition())
        elif relation_obj.type == "has_many" or relation_obj.type == "has_one":
            query.select().from_(target_table)
            query.where(relation_obj.get_join_condition())
        elif relation_obj.type == "many_to_many":
            query.select().from_(target_table)
            query.join(
                relation_obj.through_table, relation_obj.get_second_join_condition()
            )
            query.where(relation_obj.get_first_join_condition())

        return query

    @asynccontextmanager
    async def transaction(self):
        conn = None
        try:
            try:
                async with self.connection_provider.get_async_connection() as conn:
                    await conn.execute("BEGIN")
                    try:
                        yield conn
                        await conn.execute("COMMIT")
                    except Exception:
                        await conn.execute("ROLLBACK")
                        raise
            except (AttributeError, NotImplementedError):
                conn = self.connection_provider()
                if conn is None:
                    raise ValueError("Failed to establish database connection")

                conn.execute("BEGIN")
                try:
                    yield conn
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
        finally:
            if conn is not None and not hasattr(conn, "_is_from_pool"):
                if hasattr(conn, "close"):
                    conn.close()
