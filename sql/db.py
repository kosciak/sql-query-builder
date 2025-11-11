import logging
import sqlite3


log = logging.getLogger('sql.db')


class DB:

    def __init__(self, fn, tables, indexes=None):
        self.fn = fn
        self._connection = None
        self._tables = tables
        self._indexes = indexes or []

    @property
    def connection(self):
        if self._connection is None:
            self._connection = sqlite3.connect(
                self.fn,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            self._connection.row_factory = sqlite3.Row
            self._create_tables()
            self._create_indexes()
        return self._connection

    def execute_query(self, query, *params):
        # print(query)
        return self.connection.execute(
            query.sql(),
            params,
        )

    def _create_tables(self):
        for table in self._tables:
            query = self._table.create(if_not_exists=True)
            # print(query)
            self.execute_query(query)

    def _create_indexes(self):
        for index in self._indexes:
            query = index.create(if_not_exists=True)
            # print(query)
            self.execute_query(query)

