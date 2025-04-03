from typing import TypeVar, Literal, Optional, Any
from .conditions import eq

T = TypeVar("T")
TableT = TypeVar("TableT")
RelatedT = TypeVar("RelatedT")

RelationType = Literal["belongs_to", "has_one", "has_many", "many_to_many"]


class Relation:
    def __init__(
        self,
        source_table,
        target_table,
        type: RelationType,
        source_key: Optional[Any] = None,
        target_key: Optional[Any] = None,
        through_table: Optional[Any] = None,
        through_source_key: Optional[Any] = None,
        through_target_key: Optional[Any] = None,
        foreign_key: Optional[Any] = None,
    ):
        self.source_table = source_table
        self.target_table = target_table
        self.type = type
        self.through_table = through_table
        self.source_key = None
        self.target_key = None
        self.foreign_key = None
        self.through_source_key = None
        self.through_target_key = None

        if not hasattr(source_table, "_primary_keys") or not hasattr(
            source_table, "_columns"
        ):
            raise ValueError(f"Invalid source_table parameter: {source_table}")
        if not hasattr(target_table, "_primary_keys") or not hasattr(
            target_table, "_columns"
        ):
            raise ValueError(f"Invalid target_table parameter: {target_table}")

        if source_key is not None:
            self.source_key = source_key
        elif hasattr(source_table, "_primary_keys") and source_table._primary_keys:
            self.source_key = source_table._primary_keys[0]
        else:
            raise ValueError(
                f"No primary key found for source table {source_table.name}"
            )

        if target_key is not None:
            self.target_key = target_key
        elif hasattr(target_table, "_primary_keys") and target_table._primary_keys:
            self.target_key = target_table._primary_keys[0]
        else:
            raise ValueError(
                f"No primary key found for target table {target_table.name}"
            )

        if foreign_key is not None:
            self.foreign_key = foreign_key
        else:
            if type == "belongs_to":
                self._infer_belongs_to_foreign_key(source_table, target_table)
            elif type == "has_many" or type == "has_one":
                self._infer_has_many_foreign_key(source_table, target_table)

        if type == "many_to_many":
            self._setup_many_to_many_keys(
                source_table,
                target_table,
                through_table,
                through_source_key,
                through_target_key,
            )

    def _infer_belongs_to_foreign_key(self, source_table, target_table):
        if not hasattr(target_table, "name"):
            raise ValueError("Target table doesn't have a name attribute")

        target_name = target_table.name
        if target_name.endswith("s"):
            target_name = target_name[:-1]

        fk_name = f"{target_name}_id"

        for col_name, col in source_table._columns.items():
            if col_name == fk_name:
                self.foreign_key = col
                break

        if self.foreign_key is None:
            raise ValueError(
                f"Could not infer foreign key in {source_table.name} for {target_table.name}"
            )

    def _infer_has_many_foreign_key(self, source_table, target_table):
        if not hasattr(source_table, "name"):
            raise ValueError("Source table doesn't have a name attribute")

        source_name = source_table.name
        if source_name.endswith("s"):
            source_name = source_name[:-1]

        fk_name = f"{source_name}_id"

        for col_name, col in target_table._columns.items():
            if col_name == fk_name:
                self.foreign_key = col
                break

        if self.foreign_key is None:
            raise ValueError(
                f"Could not infer foreign key in {target_table.name} for {source_table.name}"
            )

    def _setup_many_to_many_keys(
        self,
        source_table,
        target_table,
        through_table,
        through_source_key,
        through_target_key,
    ):
        if through_table is None:
            raise ValueError("Through table is required for many-to-many relationship")

        if through_source_key is not None:
            self.through_source_key = through_source_key
        else:
            if not hasattr(source_table, "name"):
                raise ValueError("Source table doesn't have a name attribute")

            source_name = source_table.name
            if source_name.endswith("s"):
                source_name = source_name[:-1]

            fk_name = f"{source_name}_id"

            for col_name, col in through_table._columns.items():
                if col_name == fk_name:
                    self.through_source_key = col
                    break

            if self.through_source_key is None:
                raise ValueError(
                    f"Could not infer source foreign key in {through_table.name}"
                )

        if through_target_key is not None:
            self.through_target_key = through_target_key
        else:
            if not hasattr(target_table, "name"):
                raise ValueError("Target table doesn't have a name attribute")

            target_name = target_table.name
            if target_name.endswith("s"):
                target_name = target_name[:-1]

            fk_name = f"{target_name}_id"

            for col_name, col in through_table._columns.items():
                if col_name == fk_name:
                    self.through_target_key = col
                    break

            if self.through_target_key is None:
                raise ValueError(
                    f"Could not infer target foreign key in {through_table.name}"
                )

    def get_join_condition(self):
        if self.type == "belongs_to":
            if self.foreign_key is None or self.target_key is None:
                raise ValueError("Foreign key or target key is not defined")
            return eq(self.foreign_key, self.target_key)
        elif self.type == "has_many" or self.type == "has_one":
            if self.source_key is None or self.foreign_key is None:
                raise ValueError("Source key or foreign key is not defined")
            return eq(self.source_key, self.foreign_key)
        else:
            return None

    def get_where_condition(self, source_instance):
        if self.type == "belongs_to":
            if self.foreign_key is None or self.target_key is None:
                raise ValueError("Foreign key or target key is not defined")

            if not hasattr(source_instance, self.foreign_key.name):
                raise ValueError(
                    f"Source instance has no attribute {self.foreign_key.name}"
                )

            foreign_key_value = getattr(source_instance, self.foreign_key.name)
            return eq(self.target_key, foreign_key_value)
        elif self.type == "has_many" or self.type == "has_one":
            if self.source_key is None or self.foreign_key is None:
                raise ValueError("Source key or foreign key is not defined")

            if not hasattr(source_instance, self.source_key.name):
                raise ValueError(
                    f"Source instance has no attribute {self.source_key.name}"
                )

            source_key_value = getattr(source_instance, self.source_key.name)
            return eq(self.foreign_key, source_key_value)
        else:
            return None

    def get_first_join_condition(self):
        if self.type != "many_to_many":
            return None

        if self.source_key is None or self.through_source_key is None:
            raise ValueError("Source key or through source key is not defined")

        return eq(self.source_key, self.through_source_key)

    def get_second_join_condition(self):
        if self.type != "many_to_many":
            return None

        if self.through_target_key is None or self.target_key is None:
            raise ValueError("Through target key or target key is not defined")

        return eq(self.through_target_key, self.target_key)
