import parser

query_id = 0
index_id = 0


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

    # TODO: Consider other structures for output with more information
    def get_indexable_cols(self) -> list[str]:
        cols = set()
        for col_ident in self.attrs["filters"]:
            cols.add(col_ident)
        return list(cols)


class Column:
    def __init__(self, table: str, name: str):
        # Name of column
        self.name = name
        # Name of table
        self.table = table
        # Queries with column appearing as indexable predicate
        self.queries = set()

    def __str__(self):
        return self.name

    def get_name(self) -> str:
        return self.name

    def get_table(self) -> str:
        return self.table

    def add_query(self, qid: int):
        self.queries.add(qid)

    def get_queries(self) -> list[int]:
        return list(self.queries)


class Table:
    def __init__(self, name: str, cols: list[str]):
        self.name = name
        self.cols = dict()
        for col in cols:
            self.cols[col] = Column(self.name, col)

    def __str__(self):
        return self.name

    def get_cols(self) -> dict[str, Column]:
        return self.cols


class Index:
    def __init__(self, cols: list[Column]):
        global index_id
        assert(len(cols) > 0)
        assert(False not in [col.get_table() ==
               cols[0].get_table() for col in cols])
        # Unique ID. queryIDs are the internal, canonical representation of queries.
        self.id = index_id
        index_id += 1
        self.table = cols[0].get_table()
        self.cols = cols
        self.oid = None
        self.size = 0
        self.num_uses = 0

    def __str__(self) -> str:
        return self.create_stmt()

    def _name(self) -> str:
        return f"_tune_{self.id}"

    def _table_str(self) -> str:
        return str(self.table)

    def _cols_str(self) -> str:
        return f"{','.join([col.get_name() for col in self.cols])}"

    def get_cols(self) -> list[Column]:
        return self.cols

    def get_oid(self) -> int:
        return self.oid

    def set_oid(self, oid: int):
        self.oid = oid

    def set_size(self, size: int):
        self.size = size

    def get_size(self) -> int:
        return self.size

    def set_num_uses(self, num_uses: int) -> int:
        self.num_uses = num_uses

    def get_num_uses(self) -> int:
        return self.num_uses

    def create_stmt(self) -> str:
        return f"CREATE INDEX {self._name()} ON {self._table_str()} ({self._cols_str()})"

    def drop_stmt(self) -> str:
        return f"DROP INDEX {self._name()}"
