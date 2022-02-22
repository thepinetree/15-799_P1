from operator import index
import parser
import connector

query_id = 0
index_id = 0


class Query:
    def __init__(self, query: str, attrs: parser.QueryAttributes):
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

    def cost(self, db: connector.Connector) -> float:
        return db.get_cost(self.query)

    def get_cost(self) -> float:
        return self.best_cost

    def set_cost(self, cost: float):
        self.best_cost = cost

    def get_str(self) -> str:
        return self.query

    # TODO: Consider other structures for output with more information
    def get_indexable_cols(self) -> set(str):
        cols = set()
        for col_ident in self.attrs["filters"]:
            cols.add(col_ident)


class Column:
    def __init__(self, name: str):
        # Name of column
        self.name = name
        # Queries with column appearing as indexable predicate
        self.queries = set()

    def __str__(self):
        return self.name

    def add_query(self, qid: int):
        self.queries.add(qid)

    def get_queries(self) -> set(int):
        return self.queries


class Table:
    def __init__(self, name: str, cols: list[str]):
        self.name = name
        self.cols = set()
        for col in cols:
            self.cols.add(Column(col))

    def __str__(self):
        return self.name

    def cols(self) -> set(str):
        return self.cols


class Index:
    def __init__(self, table: Table, cols: list[Column]):
        assert(len(cols) > 0)
        # Unique ID. queryIDs are the internal, canonical representation of queries.
        self.id = index_id
        index_id += 1
        self.table = table
        self.cols = cols
        self.hyp_oid = None

    def name(self):
        return f"_tune_{self.id}"

    def cols_str(self):
        return f"{','.join(self.cols)}"

    def table_str(self):
        return str(self.table)
