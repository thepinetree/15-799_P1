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
        # Min cost improvement factor
        self.min_improvement = constants.MIN_COST_FACTOR
        # Best estimated workload cost
        self.cost = None
        # Best index under consideration
        self.next_ind = None
        # Suggested indexes to add
        self.config = []
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
            # Single index selection phase
            # TODO: remove assumption of single column index
            for ident in self.potential_cols:
                table, col = ident.split('.')
                self._evaluate_index(schema.Index(table, [col]))
            if self.next_ind is not None:
                self.config.append(self.next_ind)
                self.cost += self.delta
                # TODO: write index to output file
                logging.debug(
                    "Selecting index {}. New workload cost estimate: {}.", self.next_ind, self.cost)
                # TODO: remove assumption of single column index
                self.potential_cols.remove(self.next_ind.cols()[0])
                self.next_ind = None
                self.delta = 0
            else:  # Stop when there is no benefit to the workload
                logging.debug(
                    "Terminating selection procedure. Suggested indexes {}.", self.suggested_inds)
                break

    def _evaluate_index(self, ind: schema.Index):
        ind_oid = self.db.simulate_index(
            ind.name(), ind.table_str(), ind.cols_str())
        ind.set_oid(ind_oid)
        delta = 0
        for col in ind.get_cols():
            for qid in col.get_queries():
                new_cost = self.db.get_cost(self.queries[qid].get_str())
                delta += (new_cost - self.cost)
        # NOTE: self.delta is upper bounded by 0
        if delta < self.delta and abs(delta) >= abs(self.min_cost_factor * self.cost):
            assert(abs(delta) < abs(self.cost))
            if self.next_ind is not None:
                self.db.drop_simulated_index(self.next_ind.get_hyp_oid())
            self.next_ind = ind
            self.delta = delta
            logging.debug(
                "Index {} shows improvement. New workload cost estimate: {}.", ind, self.cost + self.delta)
        else:
            self.db.drop_simulated_index(ind_oid)
