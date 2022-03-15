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
    def __init__(self, schemas: dict[str, list[str]]):
        self.schemas = schemas

    # NOTE: Assumes that an input query is well-formatted
    def parse(self, query: str) -> QueryAttributes:
        num_queries = len(sqlparse.split(query))
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
        sets = []
        seen = KeywordType.NONE
        for token in stmt.tokens:
            if seen == KeywordType.SELECT:
                # TODO: Select does not qualify columns, must be done in a second pass. Since we
                # do not _actively_ use select cols, we ignore this for now.
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        selects.append(str(identifier))
                elif isinstance(token, sqlparse.sql.Identifier):
                    selects.append(str(token))
            if seen == KeywordType.UPDATE:
                if isinstance(token, sqlparse.sql.Identifier):
                    self._parse_table_token(tables, str(token))
            if seen == KeywordType.FROM:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        self._parse_table_token(tables, str(identifier))
                elif isinstance(token, sqlparse.sql.Identifier):
                    self._parse_table_token(tables, str(token))
            if seen == KeywordType.ORDER_BY:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        orders.append(self._qualify_column(
                            tables, self._sanitize_orderby_token(str(identifier))))
                elif isinstance(token, sqlparse.sql.Identifier):
                    orders.append(self._qualify_column(
                        tables, self._sanitize_orderby_token(str(token))))
            if seen == KeywordType.GROUP_BY:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        groups.append(self._qualify_column(
                            tables, str(identifier)))
                elif isinstance(token, sqlparse.sql.Identifier):
                    groups.append(self._qualify_column(tables, str(token)))
            if seen == KeywordType.SET:
                if (isinstance(token, sqlparse.sql.Comparison) or
                        isinstance(token, sqlparse.sql.Parenthesis)):
                    self._parse_expr_token(tables, token, sets)
                elif isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        sets.append(
                            self._qualify_column(tables, str(identifier)))
                elif isinstance(token, sqlparse.sql.Identifier):
                    sets.append(self._qualify_column(tables, str(token)))
            if isinstance(token, sqlparse.sql.Where):
                seen = KeywordType.NONE
                for where_token in token:
                    self._parse_expr_token(tables, where_token, filters)
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
            "sets": sets
        }

    def _parse_table_token(self, tables: dict[str, str], table: str):
        tokens = table.split()
        if len(tokens) == 1:
            tables[tokens[0]] = tokens[0]
            return
        assert(len(tokens) == 2)
        tables[tokens[1]] = tokens[0]

    def _sanitize_orderby_token(self, col: str) -> str:
        no_whitespace = col.rstrip()
        sanitized = no_whitespace.removesuffix(' DESC').removesuffix(' ASC')
        return sanitized

    def _parse_expr_token(self, tables: dict[str, str],
                          clause: sqlparse.sql.Token, results: set[str]):
        if isinstance(clause, sqlparse.sql.Comparison):
            for var in clause:
                if isinstance(var, sqlparse.sql.Identifier):
                    strvar = str(var)
                    results.append(self._qualify_column(tables, strvar))
        elif isinstance(clause, sqlparse.sql.Parenthesis):
            for subclause in clause.tokens:
                self._parse_expr_token(subclause, results)

    # Use table schema to prepend correct table to var in case multiple tables have same column name
    def _qualify_column(self, tables: dict[str, str], col: str) -> str:
        res = ""
        if '.' in col:
            tokens = col.split('.')
            assert(len(tokens) == 2)
            table = tables[tokens[0]]
            res = '.'.join([table, tokens[1]])
        else:
            for _, table in tables.items():
                if col in self.schemas[table]:
                    assert(res == "")
                    res = '.'.join([table, col])
        assert(res != "")
        return res


class WorkloadParser():
    def __init__(self, wf: str, schemas: dict[str, list[str]]):
        self.parser = QueryParser(schemas)
        self.input = wf

    def _is_stmt(self, q: str) -> bool:
        return "statement:" in q

    def _is_excluded(self, q: str) -> bool:
        excluded_keywords = set(["AS", "BEGIN", "COMMIT"])
        must_include = set(["SELECT", "UPDATE"])
        q_tokens = set(q.split())
        if len(excluded_keywords.intersection(q_tokens)) != 0:
            return True
        if len(must_include.intersection(q_tokens)) == 0:
            return True
        return False

    # TODO: Use a more limited preprocessing technique
    def parse_queries(self) -> list[(str, QueryAttributes)]:
        df = pandas.read_csv(self.input, sep=',', usecols=[5, 13],
                             header=None, names=["session_id", "query"])
        counts = df.groupby("session_id").aggregate("count")
        max_count = counts.max()
        thresh = 0.1 * max_count
        df = df.groupby("session_id").filter(
            lambda x: x["session_id"].count() > thresh)
        mask = df["query"].map(lambda x: self._is_stmt(x))
        df = df[mask]
        mask = df["query"].map(lambda x: not self._is_excluded(x))
        df = df[mask]
        df["query"] = df["query"].map(
            lambda x: x.removeprefix("statement: "))
        df["sanitized"] = df["query"]
        # NOTE: This fixes the weird quote removal that csv readers in python do, replacing all
        # individual single quotes with two single quotes and all individual double quotes with two
        # double quotes.
        df["sanitized"] = df["sanitized"].map(lambda x: x.replace("'", "''"))
        df["sanitized"] = df["sanitized"].map(lambda x: x.replace('"', '""'))
        # NOTE: We wish to keep the first single quote and last single quote intact to indicate
        # the edges of values
        df["sanitized"] = df["sanitized"].map(
            lambda x: x.replace("''", "'", 1))
        df["sanitized"] = df["sanitized"].map(
            lambda x: x.replace("''", "'", -1))
        # NOTE: This gets rid of problematic backslashes (e.g. in the substring \'')
        df["sanitized"] = df["sanitized"].map(lambda x: x.replace("\\", ""))
        queries = df[["query", "sanitized"]].values.tolist()
        res = []
        for val in queries:
            res.append((val[0], self.parser.parse(val[1])))
        return res


if __name__ == "__main__":
    sample_schema_epinions = {
        'item': ['i_id', 'creation_date', 'title', 'description'],
        'review': ['rating', 'u_id', 'i_id', 'a_id', 'rank', 'creation_date', 'comment'],
        'review_rating': ['u_id', 'a_id', 'rating', 'status', 'creation_date',
                          'last_mod_date', 'type', 'vertical_id'],
        'trust': ['source_u_id', 'target_u_id', 'trust', 'creation_date'],
        'useracct': ['u_id', 'creation_date', 'name', 'email']
    }
    sample_schema_jungle = {
        'jungle': ['timestamp_field9', 'int_field0', 'int_field1', 'int_field2', 'int_field3',
                   'int_field4',  'int_field5', 'int_field6', 'int_field7', 'int_field8',
                   'int_field9', 'float_field0',  'float_field1', 'float_field2', 'float_field3',
                   'float_field4', 'float_field5', 'float_field6',  'float_field7', 'float_field8',
                   'float_field9', 'timestamp_field0', 'timestamp_field1',  'timestamp_field2',
                   'timestamp_field3', 'timestamp_field4', 'timestamp_field5', 'timestamp_field6',
                   'timestamp_field7', 'timestamp_field8', 'varchar_field9', 'uuid_field',
                   'varchar_field0',  'varchar_field1', 'varchar_field2', 'varchar_field3',
                   'varchar_field4', 'varchar_field5',  'varchar_field6', 'varchar_field7',
                   'varchar_field8']
    }
    sample_schema_timeseries = {
        'sources': ['id', 'created_time', 'name', 'comment'],
        'sessions': ['id', 'source_id', 'created_time', 'agent'],
        'observations': ['source_id', 'session_id', 'type_id', 'value', 'created_time'],
        'types': ['id', 'category', 'value_type', 'name', 'comment']
    }
    # NOTE: The sample queries assume Epinions is loaded in the DB
    qp = QueryParser(sample_schema_epinions)
    res = qp.parse(
        '''
        SELECT * FROM review r, item i
        WHERE i.i_id = r.i_id and r.i_id=652
        ORDER BY rating DESC, r.creation_date DESC
        LIMIT 10
        '''
    )
    print(res)
    res = qp.parse(
        '''
        SELECT * FROM item i, review r WHERE a_id = title ORDER BY description GROUP BY a_id
        '''
    )
    print(res)
    res = qp.parse(
        '''
        UPDATE item SET title = ',lOuh%)7^Ob`\''dxFXbpV*sNN@Hlt#+z4%.h~""So%u_~q.)0WHHk,B YKsxa|@""A4X!(W@x&""x@TFnx=.<8v`h2Dbpo}XB84H{$2|+6''0xpsSasGG""""s2@^l]kw''kfaU' WHERE i_id=214
        '''
    )
    print(res)
    wp = WorkloadParser("./input/timeseries.csv", sample_schema_timeseries)
    pprint(wp.parse_queries())
