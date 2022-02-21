import parser
import connector

query_id = 0


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

    def get_cols(self) -> set(str):
        return self.cols


class Workload:
    def __init__(self):
        # Map from queryID -> Query object (attrs, cost, text)
        self.queries = dict()
        # Best estimated workload cost
        self.cost = None
        # Suggested indexes to add
        self.suggested_inds = []
        # Set of tables (and contained columns)
        self.tables = set()
        # Connector to database
        self.db = connector.Connector()

    def setup(self, wf: str):
        wp = parser.WorkloadParser()
        parsed = wp.parse_queries(wf)
        # TODO: implement get_db_info and create table structure
        for query, attrs in parsed:
            q = Query(query, attrs)
            self.queries[q.get_id()] = q
            for col_ident in q.get_indexable_cols():
                table, col = col_ident.split('.')
                # TODO: update self.tables to have new col -> query_id
