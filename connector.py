# inspired by https://github.com/hyrise/index_selection_evaluation/blob/ca1dc87e20fe64f0ef962492597b77cd1916b828/selection/dbms/postgres_dbms.py
import logging
import psycopg
import constants


class Connector():
    def __init__(self):
        self._connection = psycopg.connect(dbname=constants.DB_NAME,
                                           user=constants.DB_USER,
                                           password=constants.DB_PASS)
        self._connection.autocommit = constants.AUTOCOMMIT
        logging.debug("Connected to {} as {}",
                      constants.DB_NAME, constants.DB_USER)
        _ = self.exec_commit_no_result("CREATE EXTENSION hypopg")
        logging.debug("Enabled HypoPG")
        self.refresh_stats()

    def set_autocommit(self, autocommit: bool):
        self._connection.autocommit = autocommit

    def exec_commit_no_result(self, statement: str):
        self._connection.execute(statement)
        self._connection.commit()

    def exec_commit(self, statement: str) -> list[str]:
        cur = self._connection.execute(statement)
        results = cur.fetchall()
        self._connection.commit()
        return results

    def exec_transaction_no_result(self, statements: list[str]):
        with self._connection.transaction():
            cur = self._connection.cursor()
            for stmt in statements:
                cur.execute(stmt)
        return res

    def exec_transaction(self, statements: list[str]) -> list[list[str]]:
        res = []
        with self._connection.transaction():
            cur = self._connection.cursor()
            for stmt in statements:
                cur.execute(stmt)
                res.append(cur.fetchall())
        return res

    def close(self):
        self._connection.close()
        logging.debug("Disconnected from {} as {}",
                      constants.DB_NAME, constants.DB_USER)

    # TODO: re-type this function once an index class exists
    def simulate_index(self, index: str) -> int:
        hypopg_stmt = f"SELECT * FROM hypopg_create_index(CREATE INDEX ON {index})"
        result = self.exec_commit(hypopg_stmt)
        return result[0]

    def drop_simulated_index(self, oid: int):
        hypopg_stmt = f"SELECT * FROM hypopg_drop_index({oid})"
        result = self.exec_commit(hypopg_stmt)
        assert(result[0] == True)

    def get_cost(self, query: str) -> float:
        stmt = f"EXPLAIN (format json) {query}"
        plan = self.exec_commit(stmt)[0][0][0]["Plan"]
        cost = plan["Total Cost"]
        return cost

    def refresh_stats(self):
        self.exec_commit_no_result("ANALYZE")

    # TODO: def get_db_info


if __name__ == "__main__":
    db = Connector()
    q = "SELECT * FROM review r, item i WHERE i.i_id = r.i_id and r.i_id=112 ORDER BY rating DESC, r.creation_date DESC LIMIT 10"
    res = db.exec_commit(q)
    print(res)
    c = db.get_cost(q)
    print(c)
    oid = db.simulate_index("1(r.i_id)")
    print(oid)
    c = db.get_cost(q)
    print(c)
    db.drop_simulated_index(oid)