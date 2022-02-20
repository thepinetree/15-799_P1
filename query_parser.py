# inspired by https://stackoverflow.com/questions/58669863/is-there-any-function-to-parse-a-complete-sql-query-in-python

from enum import Enum
from typing import TypedDict

import sqlparse

# TODO: Get table schemas from Postgres
# TODO: Handle update statements


class KeywordType(Enum):
    NONE = 0
    FROM = 1
    SELECT = 2
    UPDATE = 3
    GROUP_BY = 4
    ORDER_BY = 5


class QueryAttributes(TypedDict):
    selects: list[str]
    filters: list[str]
    orders: list[str]
    groups: list[str]


class QueryParser():
    def __init__(self):
        pass

    # NOTE: Assumes that an input query is well-formatted
    def parse(self, query: str) -> QueryAttributes:
        num_queries = len(sqlparse.split(query))
        if (num_queries != 1):
            print(
                "Queries must be parsed independently. Got %d, expected 1.", num_queries)
            assert(False)
        parsed_sql = sqlparse.parse(query)
        stmt = parsed_sql[0]
        tables = []
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
                    tables.append(str(token))
            if seen == KeywordType.FROM:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        tables.append(str(token))
                elif isinstance(token, sqlparse.sql.Identifier):
                    tables.append(str(token))
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
            if isinstance(token, sqlparse.sql.Where):
                seen = KeywordType.NONE
                for where_token in token:
                    self.parse_where_token(tables, where_token, filters)
            if token.ttype is sqlparse.sql.T.Keyword and token.value.upper() == "GROUP BY":
                seen = KeywordType.GROUP_BY
            if token.ttype is sqlparse.sql.T.Keyword and token.value.upper() == "ORDER BY":
                seen = KeywordType.ORDER_BY
            if token.ttype is sqlparse.sql.T.Keyword and token.value.upper() == "FROM":
                seen = KeywordType.FROM
            if token.ttype is sqlparse.sql.T.Keyword.DML and token.value.upper() == "SELECT":
                seen = KeywordType.SELECT
            if token.ttype is sqlparse.sql.T.Keyword.DML and token.value.upper() == "UPDATE":
                seen = KeywordType.UPDATE
        return {
            "selects": selects,
            "filters": filters,
            "orders": orders,
            "groups": groups,
        }

    # TODO: Use table schema info to prepend table to var placement in filters for multiple tables
    def parse_where_token(self, tables: list[str], clause: sqlparse.sql.Token, filters: list[str]):
        if isinstance(clause, sqlparse.sql.Comparison):
            for var in clause:
                if isinstance(var, sqlparse.sql.Identifier):
                    strvar = str(var)
                    if '.' in strvar:
                        filters.append(strvar)
                    else:
                        assert(len(tables) == 1)
                        filters.append(tables[0] + '.' + strvar)
        elif isinstance(clause, sqlparse.sql.Parenthesis):
            for subclause in clause.tokens:
                self.parse_where_token(subclause, filters)


if __name__ == "__main__":
    qp = QueryParser()
    res = qp.parse(
        '''SELECT * FROM review r, item i WHERE i.i_id = r.i_id and r.i_id=112 ORDER BY rating DESC, r.creation_date DESC LIMIT 10''')
    print(res)
