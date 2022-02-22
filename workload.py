import connector
import parser
import schema


class Workload:
    def __init__(self):
        # Map from queryID -> Query object (attrs, cost, text)
        self.queries = dict()
        # Best estimated workload cost
        self.cost = None
        # Potential columns to index
        self.potential_cols = set()
        # Suggested indexes to add
        self.suggested_inds = []
        # Map from table name -> table info
        self.tables = dict()
        # Connector to database
        self.db = connector.Connector()

    def setup(self, wf: str):
        wp = parser.WorkloadParser()
        parsed = wp.parse_queries(wf)
        info = self.db.get_db_info()
        for table, cols in info:
            self.table[table] = schema.Table(table, cols)
        self.cost = 0
        for query, attrs in parsed:
            q = schema.Query(query, attrs)
            qid = q.get_id()
            self.queries[qid] = q
            for col_ident in q.get_indexable_cols():
                self.potential_cols.add(col_ident)
                table, col = col_ident.split('.')
                self.tables[table].cols()[col].add_query(qid)
            qcost = q.cost(query)
            q.set_cost(qcost)
            self.cost += qcost
