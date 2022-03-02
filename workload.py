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
        self.config = []
        # Change in cost/size
        self.improvement = 0
        # Output path for selected actions
        f = open(constants.OUTPUT_PATH, 'a')
        self.out = f
        # RAM space
        self.max_storage = psutil.virtual_memory().available
        # Iteration must terminate (dropped index)
        self.terminate_iter = False

    # Setup workload
    def setup(self, wf: str):
        # Read table information from DB
        tables = self.db.get_table_info()
        for table, cols in tables:
            self.tables[table] = schema.Table(table, cols)
        # Read index information from DB
        ind_dict = dict()
        indexes = self.db.get_index_info()
        for index, table, colnames, num_uses, size in indexes:
            cols = [self.tables[table].get_cols()[col] for col in colnames]
            ind_dict[index] = schema.Index(cols)
            ind_dict[index].set_num_uses(num_uses)
            ind_dict[index].set_size(size)
        # Sort indexes by lowest usage factor (scans / size)
        self.indexes = OrderedDict(
            sorted(ind_dict.items(), key=lambda x: x[1].get_num_uses()/x[1].get_size()))
        # Parse workload queries
        wp = parser.WorkloadParser(wf)
        parsed = wp.parse_queries()
        for query, attrs in parsed:
            q = schema.Query(query, attrs)
            qid = q.get_id()
            self.queries[qid] = q
            for col_ident in q.get_indexable_cols():
                table, col = col_ident.split('.')
                col = self.tables[table].get_cols()[col]
                col.add_query(qid)
                self.potential_cols.add(col)
        # Setup initial cost
        self.cost = self.workload_cost()
        logging.debug(f"Setup complete. Initial workload cost: {self.cost}.")

    # Run iterative selection algorithm
    def select(self):
        while not self.terminate_iter:
            # TODO: remove assumption of single column index
            # # Single index selection phase
            # Evaluate each index and choose best
            for col in self.potential_cols:
                self._evaluate_index(schema.Index([col]))
                if self.terminate_iter:
                    break
            if self.next_ind is not None:  # Index to improve workload found
                if self.next_ind.get_size() > self.max_storage:  # Over capacity, attempt rebalance
                    can_rebalance = self._rebalance_indexes(self.next_ind)
                    if can_rebalance:
                        # Stop after rebalance involving dropped index as workload costs will
                        # no longer be accurate until the index is actually dropped and workload
                        # costs are refreshed, which we rely on the execution of `actions.sql`
                        # to take care of
                        self.terminate_iter = True
                    else:
                        logging.debug(
                            "Terminating selection procedure. No remaining storage space. " +
                            f"Suggested indexes {self.config}."
                        )
                        return
                self.config.append(self.next_ind)
                self._update_costs(self.next_ind)
                # Output create index action immediately to avoid timeout
                self.out.write(self.next_ind.create_stmt() + ";\n")
                self.out.flush()
                logging.debug(
                    f"Applying '{self.next_ind}'. New workload cost estimate: {self.cost}."
                )
                # TODO: remove assumption of single column index
                self.potential_cols.remove(self.next_ind.get_cols()[0])
                self.next_ind = None
                self.improvement = 0
            else:  # Stop when there is no benefit to the workload
                logging.debug(
                    "Terminating selection procedure. No remaining cost improvement. " +
                    f"Suggested indexes {self.config}."
                )
                return
        logging.debug(
            "Terminating selection iteration. Need to reevaluate dropped index. " +
            f"Suggested indexes {self.config}."
        )

    def _workload_cost(self) -> float:
        cost = 0
        for qid, q in self.queries.items():
            query_cost = self.db.get_cost(q.query)
            cost += query_cost
            self.queries[qid].set_cost(query_cost)
        return cost

    # Evaluate index improvement
    def _evaluate_index(self, ind: schema.Index):
        # Set up simulated index info
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
        # NOTE: self.improvement is upper bounded by 0
        improvement = delta/ind_size
        if improvement < self.improvement and abs(delta) >= abs(self.min_cost_factor * self.cost):
            # Current index has best improvement and is over minimum cost improvement factor
            assert(delta < 0 and -delta < self.cost)
            if self.next_ind is not None:
                self.db.drop_simulated_index(self.next_ind.get_oid())
            self.next_ind = ind
            self.improvement = improvement
            logging.debug(
                f"Index {ind} shows improvement factor {self.improvement}. " +
                f"Cost savings: {delta}. New workload cost estimate: {self.cost + delta}."
            )
        else:
            self.db.drop_simulated_index(ind_oid)

    # If new index increases memory pressure beyond RAM capacity, consider dropping existing indexes
    # by least benefit (scans / size)
    def _rebalance_indexes(self, ind: schema.Index) -> bool:
        drop_inds = []
        ind_size = ind.get_size()
        num_uses = ind.get_num_uses()
        ind_oid = ind.get_oid()
        it = iter(self.indexes)
        while ind_size > self.max_storage:  # Continue while we need to free up more storage
            if len(self.indexes) != 0:
                # Consider the next existing index
                worst_index_name = next(it)
                w_num_uses = self.indexes[worst_index_name].get_num_uses()
                w_size = self.indexes[worst_index_name].get_size()
                # In the absence of cost information of existing indexes, we can choose to drop
                # indexes by a proxy of uses/size with the assumption that all uses are similar
                # in cost
                # TODO: try to determine a better metric of index cost than num_uses
                if num_uses/ind_size > w_num_uses/w_size:
                    drop_inds.append(worst_index_name)
                    self.max_storage -= w_size
                    continue
                # The chosen index was not better than the existing index, give up
            # There are no more indexes to consider dropping, give up
            self.db.drop_simulated_index(ind_oid)
            return False
        # Write commands to drop all chosen indexes and remove from internal set of existing indexes
        # before adding new index
        for drop_ind_name in drop_inds:
            drop_ind = self.indexes[drop_ind_name]
            self.out.write(self.drop_ind.drop_stmt() + ";\n")
            self.out.flush()
            self.max_storage -= drop_ind.get_size()
            del self.indexes[drop_ind]
        return True

    # Update workload cost and storage capacity based on single newly added index
    def _update_costs(self, ind: schema.Index):
        evaluated = set()
        delta = 0
        for col in ind.get_cols():
            for qid in col.get_queries():
                if qid not in evaluated:  # Evaluate each applicable query exactly once
                    old_cost = self.queries[qid].get_cost()
                    new_cost = self.db.get_cost(self.queries[qid].get_str())
                    delta += (new_cost - old_cost)
                    self.queries[qid].set_cost(new_cost)
                    evaluated.add(qid)
        self.cost += delta
        ind_size = self.db.size_simulated_index(ind.get_oid())
        self.max_storage -= ind_size
