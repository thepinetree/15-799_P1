# inspired by https://stackoverflow.com/questions/58669863/is-there-any-function-to-parse-a-complete-sql-query-in-python

from enum import Enum
from pprint import pprint
from typing import TypedDict

import sqlparse
import pandas


class QueryAttributes(TypedDict):
    selects: list[str]
    filters: list[str]
    orders: list[str]
    groups: list[str]
    sets: list[str]


class KeywordType(Enum):
    NONE = 0
    FROM = 1
    SELECT = 2
    UPDATE = 3
    GROUP_BY = 4
    ORDER_BY = 5
    SET = 6


class QueryParser:
    def __init__(self):
        pass

    # NOTE: Assumes that an input query is well-formatted
    def parse(self, query: str) -> QueryAttributes:
        # For whatever reason, one of sqlparse or python only occasionally does not treat
        # semi-colons well so this is a hack to fix the issue
        num_queries = len(sqlparse.split(repr(query.encode('unicode_escape'))))
        if (num_queries != 1):
            print(
                f"Queries must be parsed independently. Got {num_queries}, expected 1."
            )
            assert(False)
        parsed_sql = sqlparse.parse(query)
        stmt = parsed_sql[0]
        tables = dict()
        selects = []
        filters = []
        orders = []
        groups = []
        seen = KeywordType.NONE
        for token in stmt.tokens:
            if seen == KeywordType.SELECT:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        selects.append(str(identifier))
                elif isinstance(token, sqlparse.sql.Identifier):
                    selects.append(str(token))
            if seen == KeywordType.UPDATE:
                if isinstance(token, sqlparse.sql.Identifier):
                    self._parse_table_token(tables, token)
            if seen == KeywordType.FROM:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        self._parse_table_token(tables, identifier)
                elif isinstance(token, sqlparse.sql.Identifier):
                    self._parse_table_token(tables, token)
            if seen == KeywordType.ORDER_BY:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        orders.append(str(identifier))
                elif isinstance(token, sqlparse.sql.Identifier):
                    orders.append(str(token))
            if seen == KeywordType.GROUP_BY:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        groups.append(str(identifier))
                elif isinstance(token, sqlparse.sql.Identifier):
                    groups.append(str(token))
            if seen == KeywordType.SET:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        selects.append(str(identifier))
                elif isinstance(token, sqlparse.sql.Identifier):
                    selects.append(str(token))
            if isinstance(token, sqlparse.sql.Where):
                seen = KeywordType.NONE
                for where_token in token:
                    self._parse_where_token(tables, where_token, filters)
            if token.ttype is sqlparse.sql.T.Keyword and token.value.upper() == "FROM":
                seen = KeywordType.FROM
            if token.ttype is sqlparse.sql.T.Keyword.DML and token.value.upper() == "SELECT":
                seen = KeywordType.SELECT
            if token.ttype is sqlparse.sql.T.Keyword.DML and token.value.upper() == "UPDATE":
                seen = KeywordType.UPDATE
            if token.ttype is sqlparse.sql.T.Keyword and token.value.upper() == "GROUP BY":
                seen = KeywordType.GROUP_BY
            if token.ttype is sqlparse.sql.T.Keyword and token.value.upper() == "ORDER BY":
                seen = KeywordType.ORDER_BY
            if token.ttype is sqlparse.sql.T.Keyword and token.value.upper() == "SET":
                seen = KeywordType.SET
        return {
            "selects": selects,
            "filters": filters,
            "orders": orders,
            "groups": groups,
        }

    def _parse_table_token(self, tables: dict[str, str], table: sqlparse.sql.Token):
        tokens = str(table).split()
        if len(tokens) == 1:
            tables[tokens[0]] = tokens[0]
            return
        assert(len(tokens) == 2)
        tables[tokens[1]] = tokens[0]

    def _parse_where_token(self, tables: dict[str, str], clause: sqlparse.sql.Token, filters: list[str]):
        if isinstance(clause, sqlparse.sql.Comparison):
            for var in clause:
                if isinstance(var, sqlparse.sql.Identifier):
                    strvar = str(var)
                    if '.' in strvar:
                        tokens = strvar.split('.')
                        assert(len(tokens) == 2)
                        table = tables[tokens[0]]
                        filters.append('.'.join([table, tokens[1]]))
                    else:
                        assert(len(tables) == 1)
                        filters.append(
                            '.'.join([next(iter(tables.values())), strvar]))
        elif isinstance(clause, sqlparse.sql.Parenthesis):
            for subclause in clause.tokens:
                self._parse_where_token(subclause, filters)

    # TODO: Use table schema to prepend correct table to var in case multiple tables have column
    # def _qualify_column()


class WorkloadParser():
    def __init__(self, wf: str):
        self.parser = QueryParser()
        self.input = wf

    def _is_stmt(self, q: str) -> bool:
        return "statement:" in q

    def _is_excluded(self, q: str) -> bool:
        excluded_keywords = ["AS", "BEGIN", "COMMIT"]
        for keyword in excluded_keywords:
            if keyword in q.split():
                return True
        return False

    # TODO: Use a more limited preprocessing technique
    def parse_queries(self) -> list[(str, QueryAttributes)]:
        df = pandas.read_csv(self.input, usecols=[5, 13], header=None)
        df.columns = ["session_id", "query"]
        counts = df.groupby("session_id").aggregate('count')
        max_count = counts.max()
        thresh = 0.1 * max_count
        df = df.groupby("session_id").filter(
            lambda x: x["session_id"].count() > thresh)
        mask = df["query"].map(lambda x: self._is_stmt(x))
        df = df[mask == True]
        df["query"] = df["query"].map(lambda x: x.split("statement: ")[1])
        mask = df["query"].map(lambda x: not self._is_excluded(x))
        df = df[mask == True]
        queries = list(df["query"])
        res = []
        for query in queries:
            res.append((query, self.parser.parse(query)))
        return res


if __name__ == "__main__":
    qp = QueryParser()
    res = qp.parse(
        '''UPDATE item SET title = 'KY`U4GpwWeTqi\'']cjWVW@z" 9@N^GBOO_~;)Mt9W#z? 9@N^d8!fbty@CS<sOR$l}(+SY,*s$Cur7t#z0Exx\@GWsIlCAkXM_bK9l&ml%!P h/b=g|Yg;$2JH_w<uR7' WHERE i_id=295''')
    print(res)
    wp = WorkloadParser("./input/starter.csv")
    pprint(wp.parse_queries())
