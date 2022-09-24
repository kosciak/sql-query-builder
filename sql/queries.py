import copy
import functools
import itertools
import logging

from . import enums

from .core import get_name, to_sql
from .core import Alias, FieldsList, And


log = logging.getLogger('drewno.sql.queries')


def mutate_query(func):

    @functools.wraps(func)
    def _mutate_query(self, *args, **kwargs):
        copied = copy.copy(self)
        return func(copied, *args, **kwargs)

    return _mutate_query


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

    def _tables(self):
        yield self.table

    def _sql(self, **kwargs):
        # Yield from list of lines
        yield from []

    def _kwargs(self, **kwargs):
        kwargs = dict(kwargs)
        aliases = {}
        tables = set()
        for table in self._tables():
            if isinstance(table, Alias):
                aliases[get_name(table.target)] = table.name
                tables.add(table.target)
            else:
                tables.add(table)
        if aliases or len(tables) > 1:
            kwargs['qualified'] = True
        kwargs.setdefault('aliases', {}).update(aliases)
        return kwargs

    def sql(self, **kwargs):
        kwargs = self._kwargs(**kwargs)
        return '\n'.join(itertools.chain(self._sql(**kwargs)))

    def __copy__(self):
        newone = type(self).__new__(type(self))
        for key, value in self.__dict__.items():
            if key in {'table', 'index'}:
                newone.__dict__[key] = value
            else:
                newone.__dict__[key] = copy.copy(value)
        return newone

    def __str__(self):
        return self.sql()


class Join(Query):

    def __init__(self, table, join_type=None):
        super().__init__(table=table)
        self.join_type = join_type
        self.on_conditions = And()
        self.using_columns = FieldsList()

    def on(self, *conditions):
        self.on_conditions.extend(conditions)

    def using(self, *columns):
        self.using_columns.extend(columns)

    def _sql(self, **kwargs):
        sql = [
            f'{self.join_type and self.join_type+" " or ""}' \
            f'JOIN',
            f'    {to_sql(self.table, **kwargs)}',
        ]
        if self.on_conditions:
            sql.extend([
                f'ON',
                f'    {self.on_conditions.sql(**kwargs)}',
            ])
        if self.using_columns:
            sql.extend([
                f'USING (',
                f'    {self.using_columns.sql(**kwargs)}',
                f')',
            ])
        yield from sql


class Joinable:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.joins = []

    def _tables(self):
        yield from super()._tables()
        for join in self.joins:
            yield from join._tables()

    @mutate_query
    def join(self, table, join_type=None):
        self.joins.append(
            Join(table, join_type=join_type)
        )
        return self

    def inner_join(self, table):
        return self.join(table, join_type=enums.Join.INNER)

    def left_join(self, table):
        return self.join(table, join_type=enums.Join.LEFT)

    def right_join(self, table):
        return self.join(table, join_type=enums.Join.RIGHT)

    def full_join(self, table):
        return self.join(table, join_type=enums.Join.FULL)

    def left_outer_join(self, table):
        return self.join(table, join_type=enums.Join.LEFT_OUTER)

    def right_outer_join(self, table):
        return self.join(table, join_type=enums.Join.RIGHT_OUTER)

    def full_outer_join(self, table):
        return self.join(table, join_type=enums.Join.FULL_OUTER)

    def cross_join(self, table):
        return self.join(table, join_type=enums.Join.CROSS)

    @mutate_query
    def on(self, *conditions):
        self.joins[-1].on(*conditions)
        return self

    @mutate_query
    def using(self, *columns):
        self.joins[-1].using(*columns)
        return self

    def _sql(self, **kwargs):
        yield from super()._sql(**kwargs)
        for join in self.joins:
            yield from join._sql(**kwargs)


class RowsFiltered:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rows_conditions = And()

    @mutate_query
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

    @mutate_query
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

    @mutate_query
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

    @mutate_query
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
            f'{get_name(self.table, **kwargs)} (',
        ]
        for column in self.table.columns:
            sql.append(f'    {column.get_definition()},')
        for constraint in self.table.constraints:
            sql.append(f'    {constraint.sql(**kwargs)},')
        sql[-1] = sql[-1].rstrip(',')
        sql.append(')')
        for option in self.table.options:
            sql.append(f'{option},')
        yield from sql
        yield from super()._sql(**kwargs)


class DropTable(Query):

    def __init__(self, table, if_not_exists=False):
        super().__init__(table=table)
        self.if_not_exists = if_not_exists

    def _sql(self, **kwargs):
        sql = [
            f'DROP TABLE' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "}' \
            f'{get_name(self.table, **kwargs)}',
        ]
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
            f'{get_name(self.index, **kwargs)}',
            f'ON {get_name(self.index.table, **kwargs)} (',
            f'    {self.index.columns.sql(**kwargs)}',
            f')',
        ]
        yield from sql
        yield from super()._sql(**kwargs)


class DropIndex(Query):

    def __init__(self, index, if_not_exists=False):
        super().__init__(index=index)
        self.if_not_exists = if_not_exists

    def _sql(self, **kwargs):
        sql = [
            f'DROP INDEX' \
            f'{self.if_not_exists and " IF NOT EXISTS " or " "}' \
            f'{get_name(self.index, **kwargs)}',
        ]
        yield from sql
        yield from super()._sql(**kwargs)


class Select(Ordered, GroupsFiltered, RowsFiltered, Joinable, Query):

    ALL_COLUMNS = ['*', ]

    def __init__(self, *columns, from_table, distinct=False):
        super().__init__(table=from_table)
        self.distinct = distinct
        self.columns = FieldsList(columns or self.ALL_COLUMNS)

    def _sql(self, **kwargs):
        # kwargs['qualified'] = True
        sql = [
            f'SELECT' \
            f'{self.distinct and " DISTINCT" or ""}',
            f'    {self.columns.sql(**kwargs)}',
            f'FROM',
            f'    {to_sql(self.table, **kwargs)}',
        ]
        yield from sql
        yield from super()._sql(**kwargs)


class Insert(Query):

    def __init__(self, column_or_inserts=None, /, *columns, into_table, replace=False):
        super().__init__(table=into_table)
        self.replace = replace
        self.columns = FieldsList()
        self.values = FieldsList()
        if isinstance(column_or_inserts, dict):
            self.columns.extend(column_or_inserts.keys())
            self.values.extend(column_or_inserts.values())
        elif column_or_inserts:
            self.columns.append(column_or_inserts)
        self.columns.extend(columns)

    @mutate_query
    def values(self, *values):
        self.insert_values = FieldsList(values)
        return self

    def _sql(self, **kwargs):
        sql = [
            f'INSERT' \
            f'{self.replace and " OR REPLACE " or " "}' \
            f'INTO {get_name(self.table, **kwargs)}' \
            f'{self.columns and " (" or ""}',
        ]
        if self.columns:
            sql.extend([
                f'    {self.columns.sql(**kwargs)}',
                f')',
            ])
        if self.values:
            sql.extend([
                f'VALUES (',
                f'    {self.values.sql(**kwargs)}',
                f')',
            ])
        yield from sql
        yield from super()._sql(**kwargs)


class Update(RowsFiltered, Query):

    def __init__(self, updates=None, *, table):
        super().__init__(table=table)
        self.updates = dict(updates or {})

    @mutate_query
    def set(self, column, value):
        self.updates[column] = value
        return self

    def _sql(self, **kwargs):
        sql = [
            f'UPDATE',
            f'    {get_name(self.table, **kwargs)}',
            f'SET',
        ]
        for column, value in self.updates.items():
            sql.append(
                f'    {get_name(column, **kwargs)}={value},'
            )
        if self.updates:
            sql[-1] = sql[-1].rstrip(',')
        yield from sql
        yield from super()._sql(**kwargs)


class Delete(RowsFiltered, Query):

    def __init__(self, from_table):
        super().__init__(table=from_table)

    def _sql(self, **kwargs):
        sql = [
            f'DELETE FROM',
            f'    {get_name(self.table, **kwargs)}',
        ]
        yield from sql
        yield from super()._sql(**kwargs)

