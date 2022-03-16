from collections import OrderedDict
from pprint import pformat

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
        # Potential index configs
        self.potential_inds = set()
        # Map from table name -> table info
        self.tables = dict()
        # Map from index identifier -> index info ordered by uses/size
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
        f = open(constants.OUTPUT_PATH, 'w')
        self.out = f
        # RAM space
        self.max_storage = psutil.virtual_memory().available
        # Iteration must terminate (dropped index)
        self.terminate_iter = False

    # Setup workload
    def setup(self, wf: str):
        # Read table information from DB
        tables = self.db.get_table_info()
        for table, cols in tables.items():
            self.tables[table] = schema.Table(table, tuple(cols))
        # Read index information from DB
        ind_dict = dict()
        indexes = self.db.get_index_info()
        for name, table, colnames, num_uses, size in indexes:
            cols = [self.tables[table].get_cols()[col] for col in colnames]
            index = schema.Index(tuple(cols))
            index.set_num_uses(num_uses)
            index.set_size(size)
            index.set_name(name)
            ind_dict[index.get_identifier()] = index
        # Sort indexes by lowest usage factor (scans / size) as a proxy of their usefulness.
        # Later, if an index is actually considered to be dropped, we use a better cost metric
        # to determine if the new index is better than the worst index in this set.
        self.indexes = OrderedDict(
            sorted(ind_dict.items(), key=lambda x: x[1].get_num_uses()/x[1].get_size()))
        # Parse workload queries
        wp = parser.WorkloadParser(wf, tables)
        parsed = wp.parse_queries()
        _dbg_col_refs = set()
        for query, attrs in parsed:
            q = schema.Query(query, attrs)
            qid = q.get_id()
            self.queries[qid] = q
            for col_ident in q.get_indexable_cols():
                table, col = col_ident.split('.')
                col = self.tables[table].get_cols()[col]
                self.tables[table].add_referenced_col(col)
                col.add_query(qid)
                self.potential_inds.add(tuple([col]))
                _dbg_col_refs.add(col)
        # Setup initial cost
        self.cost = self._workload_cost()
        logging.debug("Col -> query counts: {0}".format(pformat(
            [(col.to_str(), len(col.get_queries())) for col in _dbg_col_refs]
        )))
        logging.debug("Potential indexes: {0}".format(pformat(
            [[[col.to_str() for col in cols] for cols in self.potential_inds]]
        )))
        logging.debug(f"Setup complete. Initial workload cost: {self.cost}.")

    # Run iterative selection algorithm
    def select(self):
        while not self.terminate_iter:
            # # Index selection phase
            # Evaluate each index and choose best
            for cols in self.potential_inds:
                ind = schema.Index(cols)
                if ind.get_identifier() not in self.indexes:
                    self._evaluate_index(ind)
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
                chosen_cols = self.next_ind.get_cols()
                self.potential_inds.remove(chosen_cols)
                if len(chosen_cols) < constants.MAX_INDEX_WIDTH:
                    for attr in self.tables[self.next_ind.get_table()].get_referenced_cols():
                        chosen_cols_list = list(chosen_cols)
                        if attr not in chosen_cols_list:
                            chosen_cols_list.append(attr)
                            new_ind = tuple(chosen_cols_list)
                            self.potential_inds.add(new_ind)
                            logging.debug("Adding potential index: {0}".format(
                                [col.to_str() for col in new_ind]))
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
            self.next_ind = ind
            self.improvement = improvement
            logging.debug(
                f"Index {ind} shows improvement factor {self.improvement}. " +
                f"Cost savings: {delta}. New workload cost estimate: {self.cost + delta}."
            )
        # Drop considered index before next iteration
        self.db.drop_simulated_index(ind.get_oid())

    # If new index increases memory pressure beyond RAM capacity, consider dropping existing indexes
    # by least benefit (scans / size)
    def _rebalance_indexes(self, ind: schema.Index) -> bool:
        drop_inds = []
        ind_size = ind.get_size()
        it = iter(self.indexes)
        max_storage = self.max_storage
        while ind_size > max_storage:  # Continue while we need to free up more storage
            try:
                # Consider the next existing index
                worst_index = next(it)
                if self._is_better_index(ind, self.indexes[worst_index]):
                    drop_inds.append(worst_index)
                    w_size = self.indexes[worst_index].get_size()
                    max_storage += w_size
                    continue
                # The chosen index was not better than the existing index, try again
            except StopIteration:
                # There are no more indexes to consider dropping, give up
                return False
        # Write commands to drop all chosen indexes and remove from internal set of existing indexes
        # before adding new index
        for drop_ind_ident in drop_inds:
            drop_ind = self.indexes[drop_ind_ident]
            self.out.write(drop_ind.drop_stmt() + ";\n")
            self.out.flush()
            self.max_storage += drop_ind.get_size()
            del self.indexes[drop_ind_ident]
            logging.debug(
                f"Applying '{drop_ind.drop_stmt()}'."
            )
        return True

    # Update workload cost and storage capacity based on single newly added index
    def _update_costs(self, ind: schema.Index):
        delta = self._get_index_delta(ind, True)
        self.cost += delta
        ind_size = ind.get_size()
        self.max_storage -= ind_size

    # Determine if index ind has better cost improvement than target improvement
    def _is_better_index(self, new_ind: schema.Index, old_ind: schema.Index) -> bool:
        # Temporarily simulate index drop
        self.db.simulate_index_drop(old_ind.get_name())
        delta = self._get_index_delta(new_ind, False)
        # Undo simulated index drop
        self.db.undo_simulated_index_drop(old_ind.get_name())
        # NOTE: a lower delta indicates a better cost
        if delta < 0:
            return True
        return False

    def _get_index_delta(self, ind: schema.Index, update_cost: bool) -> float:
        ind_oid = self.db.simulate_index(ind.create_stmt())
        ind.set_oid(ind_oid)
        # Evaluate cost improvement of new index
        delta = 0
        evaluated = set()
        for col in ind.get_cols():
            for qid in col.get_queries():
                if qid not in evaluated:  # Evaluate each applicable query exactly once
                    old_cost = self.queries[qid].get_cost()
                    new_cost = self.db.get_cost(self.queries[qid].get_str())
                    delta += (new_cost - old_cost)
                    evaluated.add(qid)
                    if update_cost:
                        self.queries[qid].set_cost(new_cost)
        # Drop the simulated index
        self.db.drop_simulated_index(ind.get_oid())
        return delta
