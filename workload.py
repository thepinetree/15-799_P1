import connector
import constants
import logging
import parser
import schema


class Workload:
    def __init__(self):
        # Map from queryID -> Query object (attrs, cost, text)
        self.queries = dict()
        # Potential columns to index
        self.potential_cols = set()
        # Map from table name -> table info
        self.tables = dict()
        # Connector to database
        self.db = connector.Connector()
        # Best estimated workload cost
        self.cost = None
        # Min cost improvement factor
        self.min_improvement = constants.MIN_COST_FACTOR
        # Suggested indexes to add
        self.suggested_inds = []
        # Change in cost
        self.delta = None

    def workload_cost(self) -> float:
        cost = 0
        for _, q in self.queries():
            query_cost = self.db.get_cost(q.query)
            cost += query_cost
        return cost

    def setup(self, wf: str):
        wp = parser.WorkloadParser()
        parsed = wp.parse_queries(wf)
        info = self.db.get_db_info()
        for table, cols in info:
            self.table[table] = schema.Table(table, cols)
        for query, attrs in parsed:
            q = schema.Query(query, attrs)
            qid = q.get_id()
            self.queries[qid] = q
            for col_ident in q.get_indexable_cols():
                self.potential_cols.add(col_ident)
                table, col = col_ident.split('.')
                self.tables[table].cols()[col].add_query(qid)
                self.tables[table].add_indexable_col(col)
        self.cost = self.workload_cost()
        logging.debug("Setup complete. Initial workload cost: {}.", self.cost)

    def select(self):
        while True:
            self.delta = 0
            # Single index selection phase
            for ident in self.potential_cols:
                table, col = ident.split('.')
                self.evaluate_index(schema.Index(table, [col]))
            # Stop when there is no benefit to the workload
            if self.delta >= 0:
                logging.debug(
                    "Terminating selection procedure. Suggested indexes {}.", self.suggested_inds)
                break

    def evaluate_index(self, ind: schema.Index):
        ind_oid = self.db.simulate_index(
            ind.name(), ind.table_str(), ind.cols_str())
        delta = 0
        for col in ind.cols():
            for qid in col.get_queries():
                new_cost = self.db.get_cost(self.queries[qid].get_str())
                delta += (new_cost - self.cost)
        if delta < 0 and abs(delta) >= abs(self.min_cost_factor * self.cost):
            assert(abs(delta) < abs(self.cost))
            self.cost += delta
            self.suggested_inds.append(ind)
            # TODO: remove assumption of single column index
            self.suggested_inds.remove(ind.cols()[0])
            # TODO: write index to output file
            logging.debug(
                "Adding index {}. New workload cost estimate: {}.", self.cost)
        else:
            self.db.drop_simulated_index(ind_oid)
