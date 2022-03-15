from typing import Optional

import parser

query_id = 0


class Query:
    def __init__(self, query: str, attrs: parser.QueryAttributes):
        global query_id
        # Unique ID. queryIDs are the internal, canonical representation of queries.
        self.id = query_id
        query_id += 1
        # Query string
        self.query = query
        # Query attributes
        self.attrs = attrs
        # Best estimated query cost
        self.best_cost = None

    def __str__(self):
        return self.query

    def get_id(self) -> int:
        return self.id

    def get_cost(self) -> float:
        return self.best_cost

    def set_cost(self, cost: float):
        self.best_cost = cost

    def get_str(self) -> str:
        return self.query

    def get_indexable_cols(self) -> list[str]:
        cols = set()
        for col_ident in self.attrs["filters"]:
            cols.add(col_ident)
        for col_ident in self.attrs["groups"]:
            cols.add(col_ident)
        for col_ident in self.attrs["orders"]:
            cols.add(col_ident)
        return cols


class Column:
    def __init__(self, table: str, name: str):
        # Name of column
        self.name = name
        # Name of table
        self.table = table
        # Queries with column appearing as indexable predicate
        self.queries = set()

    def to_str(self) -> str:
        return self.table + '.' + self.name

    def get_name(self) -> str:
        return self.name

    def get_table(self) -> str:
        return self.table

    def add_query(self, qid: int):
        self.queries.add(qid)

    def get_queries(self) -> list[int]:
        return list(self.queries)


class Table:
    def __init__(self, name: str, cols: tuple[str]):
        self.name = name
        self.cols = dict()
        self.referenced_cols = set()
        for col in cols:
            self.cols[col] = Column(self.name, col)

    def __str__(self):
        return self.name

    def get_cols(self) -> dict[str, Column]:
        return self.cols

    def add_referenced_col(self, col: Column):
        self.referenced_cols.add(col)

    def get_referenced_cols(self) -> set[Column]:
        return self.referenced_cols


class Index:
    class Identifier:
        def __init__(self, table: str, cols: tuple[Column, ...]):
            self.table = table
            self.cols = cols

        def __eq__(self, other):
            return self.table == other.table and self.cols == other.cols

        def __hash__(self):
            return hash((self.table, self.cols))

        def get_table(self) -> str:
            return self.table

        def get_cols(self) -> tuple[Column, ...]:
            return self.cols

        def identifier_name(self) -> str:
            return f"{self.table}__{'_'.join([col.get_name() for col in self.cols])}"

        def table_str(self) -> str:
            return self.table

        def cols_str(self) -> str:
            return f"{','.join([col.get_name() for col in self.cols])}"

    def __init__(self, cols: tuple[Column, ...]):
        global index_id
        assert(len(cols) > 0)
        assert(False not in [col.get_table() ==
               cols[0].get_table() for col in cols])
        # Unique identifier. These identifiers are the internal, canonical representation
        # of indexes.
        self.identifier = self.Identifier(cols[0].get_table(), cols)
        self.name = None
        self.oid = None
        self.size = 0
        self.num_uses = 0

    def __str__(self) -> str:
        return self.create_stmt()

    def get_cols(self) -> tuple[Column, ...]:
        return self.identifier.get_cols()

    def get_identifier(self) -> Identifier:
        return self.identifier

    def set_name(self, name: str):
        self.name = name

    def get_name(self) -> Optional[str]:
        return self.name

    def set_oid(self, oid: int):
        self.oid = oid

    def get_oid(self) -> int:
        return self.oid

    def set_size(self, size: int):
        self.size = size

    def get_size(self) -> int:
        return self.size

    def set_num_uses(self, num_uses: int) -> int:
        self.num_uses = num_uses

    def get_num_uses(self) -> int:
        return self.num_uses

    def create_stmt(self) -> str:
        name = self.name
        if name is None:
            name = self.identifier.identifier_name()
        return f"CREATE INDEX tune_{name} ON {self.identifier.table_str()} ({self.identifier.cols_str()})"  # noqa: E501

    # Only non-hypothetical indexes can be dropped, which must always use `set_name`
    def drop_stmt(self) -> str:
        assert(self.name is not None)
        return f"DROP INDEX {self.name}"
