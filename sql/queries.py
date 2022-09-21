import itertools
import logging

from .core import FieldsList, And


log = logging.getLogger('drewno.sql.queries')


# TODO: I don't like this name...
class Ordering:

    def __init__(self, *columns, order=None, nulls=None):
        self.columns = FieldsList(columns)
        self.order = order
        self.nulls = nulls

    def sql(self, **kwargs):
        return f'{self.columns.sql(**kwargs)}' \
               f'{self.order and " "+self.order or ""}' \
               f'{self.nulls and " NULLS "+self.nulls or ""}'


class Query:

    def __init__(self, table=None, index=None):
        self.table = table
        self.index = index

    def _sql(self, **kwargs):
        # Yield from list of lines
        yield from []

    def sql(self, **kwargs):
        return '\n'.join(itertools.chain(self._sql(**kwargs)))

    def __str__(self):
        return self.sql()


class RowsFiltered:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rows_conditions = And()

    def where(self, *conditions):
        self.rows_conditions.extend(conditions)
        return self

    def _sql(self, **kwargs):
        yield from super()._sql(**kwargs)
        if not self.rows_conditions:
            return
        sql = [
            f'WHERE',
            f'    {self.rows_conditions.sql(**kwargs)}',
        ]
        yield from sql


class Grouped:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_by_columns = FieldsList()

    def group_by(self, *columns):
        self.group_by_columns.extend(columns)
        return self

    def _sql(self, **kwargs):
        yield from super()._sql(**kwargs)
        if not self.group_by_columns:
            return
        sql = [
            f'GROUP BY',
            f'    {self.group_by_columns.sql(**kwargs)}',
        ]
        yield from sql


class GroupsFiltered(Grouped):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups_conditions = And()

    def having(self, *conditions):
        self.groups_conditions.extend(conditions)
        return self

    def _sql(self, **kwargs):
        yield from super()._sql(**kwargs)
        if not self.groups_conditions:
            return
        sql = [
            f'HAVING',
            f'    {self.groups_conditions.sql(**kwargs)}',
        ]
        yield from sql


class Ordered:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orderings = []

    def order_by(self, *columns, order=None, nulls=None):
        self.orderings.append(
            Ordering(*columns, order=order, nulls=nulls)
        )
        return self

    def _sql(self, **kwargs):
        yield from super()._sql(**kwargs)
        if not self.orderings:
            return
        sql = [
            f'ORDER BY',
        ]
        for ordering in self.orderings:
            sql.append(
                f'    {ordering.sql(**kwargs)},'
            )
        sql[-1] = sql[-1].rstrip(',')
        yield from sql


class CreateTable(Query):

    def __init__(self, table, if_not_exists=False):
        super().__init__(table=table)
        self.if_not_exists = if_not_exists

    def _sql(self, **kwargs):
        sql = [
            f'CREATE TABLE' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "}' \
            f'{self.table!s} (',
        ]
        for column in self.table.columns:
            sql.append(f'    {column.definition()},')
        for constraint in self.table.constraints:
            sql.append(f'    {constraint.sql(**kwargs)},')
        sql[-1] = sql[-1].rstrip(',')
        sql.append(')')
        for option in self.table.options:
            sql.append(f'{option},')
        yield from sql
        yield from super()._sql(**kwargs)


class CreateIndex(Query):

    def __init__(self, index, if_not_exists=False):
        super().__init__(index=index)
        self.if_not_exists = if_not_exists

    def _sql(self, **kwargs):
        sql = [
            f'CREATE' \
            f'{self.index.unique and " UNIQUE" or ""}' \
            f' INDEX' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "}' \
            f'{self.index!s}',
            f'ON {self.index.table!s} (',
            f'    {self.index.columns.sql(**kwargs)}',
            f')',
        ]
        yield from sql
        yield from super()._sql(**kwargs)


class Select(Ordered, GroupsFiltered, RowsFiltered, Query):

    ALL_COLUMNS = ['*', ]

    def __init__(self, table, *columns, distinct=False):
        super().__init__(table=table)
        self.distinct = distinct
        self.columns = FieldsList(columns or self.ALL_COLUMNS)

    def _sql(self, **kwargs):
        # kwargs['qualified'] = True
        sql = [
            f'SELECT' \
            f'{self.distinct and " DISTINCT" or ""}',
            f'    {self.columns.sql(**kwargs)}',
            f'FROM',
            f'    {self.table!s}',
        ]
        yield from sql
        yield from super()._sql(**kwargs)


class Insert(Query):

    def __init__(self, table, *columns, replace=False):
        super().__init__(table=table)
        self.replace = replace
        self.columns = FieldsList(columns)
        self.insert_values = FieldsList()

    def values(self, *values):
        self.insert_values = FieldsList(values)
        return self

    def _sql(self, **kwargs):
        sql = [
            f'INSERT' \
            f'{self.replace and " OR REPLACE " or " "}' \
            f'INTO {self.table!s}' \
            f'{self.columns and " (" or ""}',
        ]
        if self.columns:
            sql.extend([
                f'    {self.columns.sql(**kwargs)}',
                f')',
            ])
        if self.insert_values:
            sql.extend([
                f'VALUES (',
                f'    {self.insert_values.sql(**kwargs)}',
                f')',
            ])
        yield from sql
        yield from super()._sql(**kwargs)


class Update(RowsFiltered, Query):

    def __init__(self, table):
        super().__init__(table=table)
        self.values = []

    def set(self, column, value):
        self.values.append((column, value))
        return self

    def _sql(self, **kwargs):
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
        yield from super()._sql(**kwargs)


class Delete(RowsFiltered, Query):

    def _sql(self, **kwargs):
        sql = [
            f'DELETE FROM',
            f'    {self.table!s}',
        ]
        yield from sql
        yield from super()._sql(**kwargs)

