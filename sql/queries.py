import itertools
import logging

from .utils import fields


log = logging.getLogger('drewno.sql.queries')


class Query:

    def __init__(self, table=None, index=None):
        self.table = table
        self.index = index

    def _sql(self):
        # Yield from list of lines
        yield from []

    def sql(self):
        return '\n'.join(itertools.chain(self._sql()))

    def __str__(self):
        return self.sql()


class RowsFiltered:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rows_conditions = []

    def where(self, *conditions):
        self.rows_conditions.extend(conditions)
        return self

    def _sql(self):
        yield from super()._sql()
        if not self.rows_conditions:
            return
        conditions = ' AND '.join(f'{condition!s}' for condition in self.rows_conditions)
        sql = [
            f'WHERE',
            f'    {conditions}',
        ]
        yield from sql


class Grouped:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_by_columns = []

    def group_by(self, *columns):
        self.group_by_columns.extend(columns)
        return self

    def _sql(self):
        yield from super()._sql()
        if not self.group_by_columns:
            return
        sql = [
            f'GROUP BY',
            f'    {fields(self.group_by_columns)}',
        ]
        yield from sql


class GroupsFiltered(Grouped):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups_conditions = []

    def having(self, *conditions):
        self.groups_conditions.extend(conditions)
        return self

    def _sql(self):
        yield from super()._sql()
        if not self.groups_conditions:
            return
        conditions = ' AND '.join(f'{condition!s}' for condition in self.groups_conditions)
        sql = [
            f'HAVING',
            f'    {conditions}',
        ]
        yield from sql


class Ordered:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.order_by_columns = []

    def order_by(self, *columns, order=None, nulls=None):
        self.order_by_columns.append([columns, order, nulls])
        return self

    def _sql(self):
        yield from super()._sql()
        if not self.order_by_columns:
            return
        sql = [
            f'ORDER BY',
        ]
        for columns, order, nulls in self.order_by_columns:
            sql.append(
                f'    {fields(columns)}' \
                f'{order and " "+order or ""}' \
                f'{nulls and " NULLS "+nulls or ""}' \
                f',',
            )
        sql[-1] = sql[-1].rstrip(',')
        yield from sql


class CreateTable(Query):

    def __init__(self, table, if_not_exists=False):
        super().__init__(table=table)
        self.if_not_exists = if_not_exists

    def _sql(self):
        sql = [
            f'CREATE TABLE' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "} ' \
            f'{self.table!s} (',
        ]
        for column in self.table.columns:
            sql.append(f'    {column.sql()},')
        for constraint in self.table.constraints:
            sql.append(f'    {constraint},')
        for option in self.table.options:
            sql.append(f'    {option},')
        sql[-1] = sql[-1].rstrip(',')
        sql.append(')')
        yield from sql
        yield from super()._sql()


class CreateIndex(Query):

    def __init__(self, index, if_not_exists=False):
        super().__init__(index=index)
        self.if_not_exists = if_not_exists

    def _sql(self):
        sql = [
            f'CREATE' \
            f'{self.index.unique and " UNIQUE" or ""}' \
            f' INDEX' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "}' \
            f'{self.index!s}',
            f'ON {self.index.table!s} (',
            f'    {fields(self.index.columns)}',
            f')',
        ]
        yield from sql
        yield from super()._sql()


class Select(Ordered, GroupsFiltered, RowsFiltered, Query):

    ALL_COLUMNS = ['*', ]

    def __init__(self, table, *columns, distinct=False):
        super().__init__(table=table)
        self.distinct = distinct
        self.columns = columns or self.ALL_COLUMNS

    def _sql(self):
        sql = [
            f'SELECT' \
            f'{self.distinct and " DISTINCT" or ""}',
            f'    {fields(self.columns)}',
            f'FROM',
            f'    {self.table!s}',
        ]
        yield from sql
        yield from super()._sql()


class Insert(Query):

    def __init__(self, table, *columns, replace=False):
        super().__init__(table=table)
        self.replace = replace
        self.columns = columns
        self.insert_values = []

    def values(self, *values):
        self.insert_values = values
        return self

    def _sql(self):
        sql = [
            f'INSERT' \
            f'{self.replace and " OR REPLACE " or " "}' \
            f'INTO {self.table!s}' \
            f'{self.columns and " (" or ""}',
        ]
        if self.columns:
            sql.extend([
                f'    {fields(self.columns)}',
                f')',
            ])
        if self.insert_values:
            sql.extend([
                f'VALUES (',
                f'    {fields(self.insert_values)}',
                f')',
            ])
        yield from sql
        yield from super()._sql()


class Update(RowsFiltered, Query):

    def __init__(self, table):
        super().__init__(table=table)
        self.values = []

    def set(self, column, value):
        self.values.append((column, value))
        return self

    def _sql(self):
        sql = [
            f'UPDATE',
            f'    {self.table!s}',
            f'SET',
        ]
        for column, value in self.values:
            sql.append(
                f'    {column!s}={value},'
            )
        if self.values:
            sql[-1] = sql[-1].rstrip(',')
        yield from sql
        yield from super()._sql()


class Delete(RowsFiltered, Query):

    def _sql(self):
        sql = [
            f'DELETE FROM',
            f'    {self.table!s}',
        ]
        yield from sql
        yield from super()._sql()

