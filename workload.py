from collections import OrderedDict
import connector
import constants
import logging
import parser
import psutil
import schema


class Workload:
    def __init__(self):
        # Map from queryID -> Query object (attrs, cost, text)
        self.queries = dict()
        # Potential columns to index
        self.potential_cols = set()
        # Map from table name -> table info
        self.tables = dict()
        # Map from index name -> index info ordered by uses/size
        self.indexes = OrderedDict()
        # Connector to database
        self.db = connector.Connector()
        # Min cost improvement factor
        self.min_cost_factor = constants.MIN_COST_FACTOR
        # Best estimated workload cost
        self.cost = None
        # Best index under consideration
        self.next_ind = None
        # Suggested indexes to add
        # TODO: consider change to dictionary
        self.config = []
        # Change in cost/size
        self.improvement = 0
        # Output path for selected actions
        f = open(constants.OUTPUT_PATH, 'a')
        self.out = f
        # RAM space
        self.max_storage = psutil.virtual_memory().available

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
        tables = self.db.get_table_info()
        for table, cols in tables:
            self.tables[table] = schema.Table(table, cols)
        ind_dict = dict()
        indexes = self.db.get_index_info()
        for index, table, colnames, num_uses, size in indexes:
            cols = [self.tables[table].get_cols()[col] for col in colnames]
            ind_dict[index] = schema.Index(cols)
            ind_dict[index].set_num_uses(num_uses)
            ind_dict[index].set_size(size)
            self.indexes = OrderedDict(
                sorted(ind_dict.items(), key=lambda x: x[1].get_num_uses()/x[1].get_size()))
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
                self.out.write(self.next_ind.create_stmt() + ";\n")
                self.out.flush()
                logging.debug(
                    f"Applying '{self.next_ind}'. New workload cost estimate: {self.cost}.")
                # TODO: remove assumption of single column index
                self.potential_cols.remove(self.next_ind.get_cols()[0])
                self.next_ind = None
                self.improvement = 0
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
        ind_size = self.db.size_simulated_index(ind.get_oid())
        self.max_storage -= ind_size

    def _evaluate_index(self, ind: schema.Index):
        ind_oid = self.db.simulate_index(ind.create_stmt())
        ind.set_oid(ind_oid)
        ind_size = self.db.size_simulated_index(ind_oid)
        ind.set_size(ind_size)
        num_uses = 0
        delta = 0
        evaluated = set()
        for col in ind.get_cols():
            for qid in col.get_queries():
                num_uses += 1
                if qid not in evaluated:
                    old_cost = self.queries[qid].get_cost()
                    new_cost = self.db.get_cost(self.queries[qid].get_str())
                    delta += (new_cost - old_cost)
                    evaluated.add(qid)
        ind.set_num_uses(num_uses)
        # We are over storage capacity
        drop_inds = []
        it = iter(self.indexes)
        max_storage = self.max_storage
        while ind_size > max_storage:
            if len(self.indexes) != 0:
                worst_index = next(it)
                w_num_uses = self.indexes[worst_index].get_num_uses()
                w_size = self.indexes[worst_index].get_size()
                # In the absense of cost information of existing indexes, we can choose to drop indexes by a proxy of
                # uses/size assuming the uses are approximately similar in cost
                # TODO: find a better metric of index cost
                if num_uses/ind_size > w_num_uses/w_size:
                    drop_inds.append(worst_index)
                    max_storage -= w_size
                    continue
            self.db.drop_simulated_index(ind_oid)
            return
        for drop_ind in drop_inds:
            drop_ind = self.indexes[drop_ind]
            self.out.write(self.next_ind.drop_stmt() + ";\n")
            self.out.flush()
            self.max_storage -= drop_ind.get_size()
            del self.indexes[drop_ind]
        # NOTE: self.improvement is upper bounded by 0
        improvement = delta/ind_size
        if improvement < self.improvement and abs(delta) >= abs(self.min_cost_factor * self.cost):
            assert(delta < 0)
            assert(-delta < self.cost)
            if self.next_ind is not None:
                self.db.drop_simulated_index(self.next_ind.get_oid())
            self.next_ind = ind
            self.improvement = improvement
            logging.debug(
                f"Index {ind} shows improvement factor {self.improvement}. Cost savings: {delta}. New workload cost estimate: {self.cost + delta}.")
        else:
            self.db.drop_simulated_index(ind_oid)
