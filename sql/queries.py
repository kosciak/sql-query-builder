import itertools
import logging

from .utils import columns_list


log = logging.getLogger('drewno.sql.queries')


class Query:

    def __init__(self, table=None, index=None):
        self.table = table
        self.index = index

    def sql(self):
        return '\n'.join(itertools.chain(
            self._create_table_sql(),
        ))

    def __str__(self):
        return self.sql()


class WithConditions:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conditions = []

    def where(self, *conditions):
        self.conditions.extend(conditions)
        return self

    def _where_sql(self):
        if not self.conditions:
            return []
        conditions = ' AND '.join(f'{condition!s}' for condition in self.conditions)
        sql = [
            f'WHERE',
            f'    {conditions}',
        ]
        return sql


class CreateTable(Query):

    def __init__(self, table, if_not_exists=False):
        super().__init__(table=table)
        self.if_not_exists = if_not_exists

    def _create_table_sql(self):
        sql = [
            f'CREATE TABLE' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "} ' \
            f'{self.table!s} (',
        ]
        for column in self.table.columns:
            sql.append(f'    {column.definition},')
        for constraint in self.table.constraints:
            sql.append(f'    {constraint},')
        for option in self.table.options:
            sql.append(f'    {option},')
        sql[-1] = sql[-1].rstrip(',')
        sql.append(')')
        return sql

    def sql(self):
        return '\n'.join(itertools.chain(
            self._create_table_sql(),
        ))


class CreateIndex(Query):

    def __init__(self, index, if_not_exists=False):
        super().__init__(index=index)
        self.if_not_exists = if_not_exists

    def _create_index_sql(self):
        sql = [
            f'CREATE' \
            f'{self.index.unique and " UNIQUE" or ""}' \
            f' INDEX' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "}' \
            f'{self.index!s}',
            f'ON {self.index.table!s} (',
            f'    {columns_list(self.index.columns)}',
            f')',
        ]
        return sql

    def sql(self):
        return '\n'.join(itertools.chain(
            self._create_index_sql(),
        ))


class Select(WithConditions, Query):

    ALL_COLUMNS = ['*', ]

    def __init__(self, table, *columns):
        super().__init__(table=table)
        self.columns = columns or self.ALL_COLUMNS
        self.group_by_columns = []

    def group_by(self, *columns):
        self.group_by_columns.extend(columns)
        return self

    def _select_from_sql(self):
        sql = [
            f'SELECT',
            f'    {columns_list(self.columns)}',
            f'FROM',
            f'    {self.table!s}',
        ]
        return sql

    def _group_by_sql(self):
        if not self.group_by_columns:
            return []
        sql = [
            f'GROUP BY',
            f'    {columns_list(self.group_by_columns)}',
        ]
        return sql

    def sql(self):
        return '\n'.join(itertools.chain(
            self._select_from_sql(),
            self._where_sql(),
            self._group_by_sql(),
        ))


class Insert(Query):

    def __init__(self, table, *columns, replace=False):
        super().__init__(table=table)
        self.replace = replace
        self.columns = columns
        self.insert_values = []

    def values(self, *values):
        self.insert_values = values
        return self

    def _insert_into_sql(self):
        sql = [
            f'INSERT' \
            f'{self.replace and " OR REPLACE " or " "}' \
            f'INTO {self.table!s}' \
            f'{self.columns and " (" or ""}',
        ]
        if self.columns:
            sql.extend([
                f'    {columns_list(self.columns)}',
                f')',
            ])
        if self.insert_values:
            sql.extend([
                f'VALUES (',
                f'    {", ".join(self.insert_values)}',
                f')',
            ])
        return sql

    def sql(self):
        return '\n'.join(itertools.chain(
            self._insert_into_sql(),
        ))


class Update(WithConditions, Query):

    def __init__(self, table):
        super().__init__(table=table)
        self.values = []

    def set(self, column, value):
        self.values.append((column, value))
        return self

    def _update_sql(self):
        sql = [
            f'UPDATE',
            f'    {self.table!s}',
        ]
        return sql

    def _set_sql(self):
        sql = [
            f'SET',
        ]
        for column, value in self.values:
            sql.append(
                f'    {column!s}={value},'
            )
        if self.values:
            sql[-1] = sql[-1].rstrip(',')
        return sql

    def sql(self):
        return '\n'.join(itertools.chain(
            self._update_sql(),
            self._set_sql(),
            self._where_sql(),
        ))


class Delete(WithConditions, Query):

    def _delete_from_sql(self):
        sql = [
            f'DELETE FROM',
            f'    {self.table!s}',
        ]
        return sql

    def sql(self):
        return '\n'.join(itertools.chain(
            self._delete_from_sql(),
            self._where_sql(),
        ))

