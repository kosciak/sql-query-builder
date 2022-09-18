import logging

from . import queries
from .utils import fields


log = logging.getLogger('drewno.sql.core')


class Order:
    ASC = 'ASC'
    DESC = 'DESC'


class Nulls:
    FIRST = 'FIRST'
    LASR = 'LAST'


class Alias:

    def __init__(self, name, alias):
        self.name = name
        self.alias = alias

    def __str__(self):
        return f'{self.name!s} AS {self.alias!s}'


class Field:

    def __init__(self, name):
        self.name = name

    def __add__(self, value):
        return Field(f'{self!s} + {value!s}')

    def __sub__(self, value):
        return Field(f'{self!s} - {value!s}')

    def __mul__(self, value):
        return Field(f'{self!s} * {value!s}')

    def __truediv__(self, value):
        return Field(f'{self!s} / {value!s}')

    def __mod__(self, value):
        return Field(f'{self!s} % {value!s}')

    def __and__(self, value):
        return Field(f'{self!s} & {value!s}')

    def __or__(self, value):
        return Field(f'{self!s} | {value!s}')

    def __xor__(self, value):
        return Field(f'{self!s} ^ {value!s}')

    def __eq__(self, value):
        return Field(f'{self!s} = {value!s}')

    def __ne__(self, value):
        return Field(f'{self!s} != {value!s}')

    def __lt__(self, value):
        return Field(f'{self!s} < {value!s}')

    def __le__(self, value):
        return Field(f'{self!s} <= {value!s}')

    def __gt__(self, value):
        return Field(f'{self!s} > {value!s}')

    def __ge__(self, value):
        return Field(f'{self!s} >= {value!s}')

    def __str__(self):
        return self.name


class Column(Field):

    def __init__(self, name, data_type=None, constraints=None):
        super().__init__(name=name)
        self.data_type = data_type
        self.constraints = constraints

    @classmethod
    def parse(cls, column):
        if isinstance(column, cls):
            return column
        parts = [part.strip() for part in column.split(maxsplit=2) if part.strip()]
        name = parts[0]
        data_type = None
        constraints = None
        if len(parts) > 1:
            data_type = parts[1]
        if len(parts) > 2:
            constraints = parts[2]
        return Column(name, data_type, constraints)

    def sql(self):
        definition = [self.name, ]
        if self.data_type:
            definition.append(self.data_type)
        if self.constraints:
            definition.append(self.constraints)
        return " ".join(definition)

    def __repr__(self):
        return f'Column("{self.definition}")'


class Columns:

    def __init__(self, *columns):
        self._columns = self._parse_columns(columns)

    def _parse_columns(self, columns):
        parsed_columns = {}
        for column in columns:
            column = Column.parse(column)
            parsed_columns[column.name] = column
        return parsed_columns

    def __getattr__(self, column):
        return self._columns[column]

    def __iter__(self):
        yield from self._columns.values()

    def __repr__(self):
        return f'<{self.__class__.__name__} {fields(self)}>'


class Table:

    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.constraints = []
        self.options = []

    def primary_key(self, *columns):
        self.constraints.append(
            f"PRIMARY KEY ({fields(columns)})"
        )
        return self

    def unique(self, *columns):
        self.constraints.append(
            f"UNIQUE ({fields(columns)})"
        )
        return self

    def create(self, if_not_exists=False):
        return queries.CreateTable(self, if_not_exists)

    # TODO: def alter(self, ...):
    # TODO: def drop(self, ...):

    def index(self, name, *columns, unique=False):
        return Index(name, self, *columns, unique=unique)

    def select(self, *columns, distinct=False):
        return queries.Select(self, *columns, distinct=distinct)

    def insert(self, *columns, replace=False):
        return queries.Insert(self, *columns, replace=replace)

    def update(self):
        return queries.Update(self)

    def delete(self):
        return queries.Delete(self)

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'<{self.__class__.__name__} name="{self.name}">'


class Index:

    def __init__(self, name, table, *columns, unique=False):
        self.name = name
        self.table = table
        self.unique = unique
        self.columns = columns

    def create(self, if_not_exists=False):
        return queries.CreateIndex(self, if_not_exists)

    # TODO: def drop(self, ...):

    def __str__(self):
        return self.name

    # TODO: def __repr__(self):


class Condition:

    @staticmethod
    def eq(column):
        return f'{column!s} = ?'

    @staticmethod
    def ne(column):
        return f'{column!s} != ?'

    @staticmethod
    def lt(column):
        return f'{column!s} < ?'

    @staticmethod
    def le(column):
        return f'{column!s} <= ?'

    @staticmethod
    def gt(column):
        return f'{column!s} > ?'

    @staticmethod
    def ge(column):
        return f'{column!s} >= ?'

