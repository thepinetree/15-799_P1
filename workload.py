from pprint import pprint
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
        self.delta = 0

    def workload_cost(self) -> float:
        cost = 0
        for qid, q in self.queries.items():
            query_cost = self.db.get_cost(q.query)
            cost += query_cost
            self.queries[qid].set_cost(query_cost)
        return cost

    def setup(self, wf: str):
        wp = parser.WorkloadParser(wf)
        parsed = wp.parse_queries()
        info = self.db.get_db_info()
        for table, cols in info:
            self.tables[table] = schema.Table(table, cols)
        for query, attrs in parsed:
            q = schema.Query(query, attrs)
            qid = q.get_id()
            self.queries[qid] = q
            for col_ident in q.get_indexable_cols():
                table, col = col_ident.split('.')
                col = self.tables[table].get_cols()[col]
                col.add_query(qid)
                self.potential_cols.add(col)
        self.cost = self.workload_cost()
        logging.debug(f"Setup complete. Initial workload cost: {self.cost}.")

    def select(self):
        while True:
            # Single index selection phase
            # TODO: remove assumption of single column index
            for col in self.potential_cols:
                self._evaluate_index(schema.Index([col]))
            if self.next_ind is not None:
                self.config.append(self.next_ind)
                self._update_costs(self.next_ind)
                # TODO: write index to output file
                logging.debug(
                    f"Selecting index {self.next_ind}. New workload cost estimate: {self.cost}.")
                # TODO: remove assumption of single column index
                self.potential_cols.remove(self.next_ind.get_cols()[0])
                self.next_ind = None
                self.delta = 0
            else:  # Stop when there is no benefit to the workload
                logging.debug(
                    f"Terminating selection procedure. Suggested indexes {self.config}.")
                break

    def _update_costs(self, ind: schema.Index):
        evaluated = set()
        delta = 0
        for col in ind.get_cols():
            for qid in col.get_queries():
                if qid not in evaluated:
                    old_cost = self.queries[qid].get_cost()
                    new_cost = self.db.get_cost(self.queries[qid].get_str())
                    delta += (new_cost - old_cost)
                    self.queries[qid].set_cost(new_cost)
                    evaluated.add(qid)
        self.cost += delta

    def _evaluate_index(self, ind: schema.Index):
        ind_oid = self.db.simulate_index(
            ind.name(), ind.table_str(), ind.cols_str())
        ind.set_hyp_oid(ind_oid)
        delta = 0
        evaluated = set()
        for col in ind.get_cols():
            for qid in col.get_queries():
                if qid not in evaluated:
                    old_cost = self.queries[qid].get_cost()
                    new_cost = self.db.get_cost(self.queries[qid].get_str())
                    delta += (new_cost - old_cost)
                    evaluated.add(qid)
        # NOTE: self.delta is upper bounded by 0
        if delta < self.delta and abs(delta) >= abs(self.min_improvement * self.cost):
            assert(delta < 0)
            assert(-delta < self.cost)
            if self.next_ind is not None:
                self.db.drop_simulated_index(self.next_ind.get_hyp_oid())
            self.next_ind = ind
            self.delta = delta
            logging.debug(
                f"Index {ind} shows improvement. Cost savings: {self.delta}. New workload cost estimate: {self.cost + self.delta}.")
        else:
            self.db.drop_simulated_index(ind_oid)
