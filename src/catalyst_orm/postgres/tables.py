from typing import (
    Dict,
    TypeVar,
    Generic,
    Any,
    Optional,
    List,
    ClassVar,
)
from .column_types import PostgresType

T = TypeVar("T")
TableT = TypeVar("TableT", bound="PgTable")


class _Column(Generic[T]):
    def __init__(self, sql_type: PostgresType, name: Optional[str] = None):
        self.sql_type = sql_type
        self.name = name
        self.table: Optional[PgTable] = None
        self._is_primary = False
        self._is_unique = False
        self._is_nullable = True
        self._has_default = False
        self._default_value = None
        self._default_expr = None

        sql_type.extends_column(self)

    def primary(self) -> "_Column[T]":
        self._is_primary = True
        self._is_nullable = False
        return self

    def primary_key(self) -> "_Column[T]":
        return self.primary()

    def unique(self) -> "_Column[T]":
        self._is_unique = True
        return self

    def not_null(self) -> "_Column[T]":
        self._is_nullable = False
        return self

    def nullable(self) -> "_Column[T]":
        self._is_nullable = True
        return self

    def default(self, value: T) -> "_Column[T]":
        self._has_default = True
        self._default_value = value
        self._default_expr = None
        return self

    def default_sql(self, expr: str) -> "_Column[T]":
        self._has_default = True
        self._default_value = None
        self._default_expr = expr
        return self

    def alias(self, alias_name: str) -> "_ColumnAlias[T]":
        return _ColumnAlias(self, alias_name)

    def __repr__(self) -> str:
        table_name = self.table.name if self.table else "unbound"
        return f"<Column {table_name}.{self.name}>"

    def __str__(self) -> str:
        if self.table is None:
            return self.name if self.name is not None else ""

        column_name = self.name if self.name is not None else ""

        if self.table is not None:
            table_name = self.table.__tablename__
        else:
            table_name = "unknown"

        return f"{table_name}.{column_name}"


class _ColumnAlias(Generic[T]):
    def __init__(self, column: _Column[T], alias: str):
        self.column = column
        self.alias = alias
        self.table = column.table
        self.name = column.name


class RelationReference:
    def __init__(self, relation_name: str, relation):
        self.relation_name = relation_name
        self.relation = relation

    @property
    def source_table(self):
        return self.relation.source_table

    @property
    def target_table(self):
        return self.relation.target_table

    def __repr__(self):
        source_name = (
            self.source_table.name if hasattr(self.source_table, "name") else "unknown"
        )
        target_name = (
            self.target_table.name if hasattr(self.target_table, "name") else "unknown"
        )
        return f"<Relation {source_name}.{self.relation_name} -> {target_name}>"


class TableMetaclass(type):
    def __new__(mcs, name, bases, attrs):
        if name == "Table" and not bases:
            return super().__new__(mcs, name, bases, attrs)

        table_name = attrs.get("__tablename__")
        if table_name is None:
            import re

            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
            table_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

        columns = {}
        primary_keys = []

        for key, value in list(attrs.items()):
            if isinstance(value, _Column):
                if value.name is None:
                    value.name = key

                columns[key] = value

                if value._is_primary:
                    primary_keys.append(value)

                attrs.pop(key)

        attrs["__columns__"] = columns
        attrs["__tablename__"] = table_name
        attrs["__primary_keys__"] = primary_keys

        cls = super().__new__(mcs, name, bases, attrs)

        return cls


class PgTable(metaclass=TableMetaclass):
    __tablename__: ClassVar[Optional[str]] = None
    __columns__: ClassVar[Dict[str, _Column]] = {}
    __primary_keys__: ClassVar[List[_Column]] = []

    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__tablename__
        self._columns = {}
        self._primary_keys = []
        self._relations = {}

        for attr_name, column in self.__class__.__columns__.items():
            column_copy = _Column(column.sql_type, column.name)
            column_copy.table = self
            column_copy._is_primary = column._is_primary
            column_copy._is_unique = column._is_unique
            column_copy._is_nullable = column._is_nullable
            column_copy._has_default = column._has_default
            column_copy._default_value = column._default_value
            column_copy._default_expr = column._default_expr

            self._columns[column.name] = column_copy
            setattr(self, attr_name, column_copy)

            if column_copy._is_primary:
                self._primary_keys.append(column_copy)

    def column(self, name: str, sql_type: PostgresType) -> _Column[Any]:
        column_instance = _Column(sql_type, name)
        column_instance.table = self
        self._columns[name] = column_instance
        return column_instance

    def get_columns(self) -> Dict[str, _Column[Any]]:
        return self._columns

    def get_column_names(self) -> List[str]:
        return list(self._columns.keys())

    def get_create_table_sql(self) -> str:
        column_defs = []
        constraints = []

        for name, column in self._columns.items():
            column_def = f"{name} {column.sql_type.sql_type_name}"

            if not column._is_nullable:
                column_def += " NOT NULL"

            if column._has_default:
                if column._default_expr is not None:
                    column_def += f" DEFAULT {column._default_expr}"
                elif column._default_value is None:
                    column_def += " DEFAULT NULL"
                elif isinstance(column._default_value, str):
                    column_def += f" DEFAULT '{column._default_value}'"
                else:
                    column_def += f" DEFAULT {column._default_value}"

            column_defs.append(column_def)

            if column._is_unique:
                constraints.append(
                    f"CONSTRAINT {self.name}_{name}_unique UNIQUE ({name})"
                )

        if self._primary_keys:
            pk_names = [pk.name for pk in self._primary_keys]
            constraints.append(f"PRIMARY KEY ({', '.join(pk_names)})")

        all_parts = column_defs + constraints
        return f"CREATE TABLE {self.name} (\n  " + ",\n  ".join(all_parts) + "\n);"

    def has_one(self, relation_name: str, target_table: "PgTable", **kwargs):
        from ..relationships import Relation

        relation = Relation(
            source_table=self, target_table=target_table, type="has_one", **kwargs
        )
        self._relations[relation_name] = relation

        relation_ref = RelationReference(relation_name, relation)
        setattr(self, relation_name, relation_ref)
        return relation

    def has_many(self, relation_name: str, target_table: "PgTable", **kwargs):
        from ..relationships import Relation

        relation = Relation(
            source_table=self, target_table=target_table, type="has_many", **kwargs
        )
        self._relations[relation_name] = relation

        relation_ref = RelationReference(relation_name, relation)
        setattr(self, relation_name, relation_ref)
        return relation

    def belongs_to(self, relation_name: str, target_table: "PgTable", **kwargs):
        from ..relationships import Relation

        relation = Relation(
            source_table=self, target_table=target_table, type="belongs_to", **kwargs
        )
        self._relations[relation_name] = relation

        relation_ref = RelationReference(relation_name, relation)
        setattr(self, relation_name, relation_ref)
        return relation

    def many_to_many(
        self,
        relation_name: str,
        target_table: "PgTable",
        through_table: "PgTable",
        **kwargs,
    ):
        from ..relationships import Relation

        relation = Relation(
            source_table=self,
            target_table=target_table,
            type="many_to_many",
            through_table=through_table,
            **kwargs,
        )
        self._relations[relation_name] = relation

        relation_ref = RelationReference(relation_name, relation)
        setattr(self, relation_name, relation_ref)
        return relation

    def __repr__(self) -> str:
        return f"<Table {self.__tablename__}>"

    def __str__(self) -> str:
        return self.__tablename__ or ""


def column(sql_type: PostgresType) -> _Column:
    return _Column(sql_type)
