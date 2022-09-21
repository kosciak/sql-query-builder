import logging

from .utils import fields
from .core import Field, FieldsList
from . import queries


log = logging.getLogger('drewno.sql.core')


class Column(Field):

    def __init__(self, name, data_type=None, constraints=None):
        super().__init__(name=name)
        self.data_type = data_type
        self.constraints = constraints

    @property
    def table(self):
        return self.parent

    @table.setter
    def table(self, table):
        self.parent = table

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

    def definition(self):
        definition = [self.name, ]
        if self.data_type:
            definition.append(self.data_type)
        if self.constraints:
            definition.append(self.constraints)
        return " ".join(definition)

    def __repr__(self):
        return f'Column("{self.definition()}")'


class Columns:

    def __init__(self, *columns):
        self.__parent = None
        self.__columns = self._parse_columns(columns)

    def _parse_columns(self, columns):
        parsed_columns = {}
        for column in columns:
            column = Column.parse(column)
            parsed_columns[column.name] = column
        return parsed_columns

    def set_table(self, table):
        for column in self:
            column.table = table
        self.__parent = table
        return self

    def get_name(self, **kwargs):
        return '*'

    def sql(self, **kwargs):
        return self.get_name(**kwargs)

    def __getattr__(self, column):
        return self.__columns[column]

    def __iter__(self):
        yield from self.__columns.values()

    def __repr__(self):
        return f'<{self.__class__.__name__} {fields(self)}>'


class TableConstraint:

    def __init__(self, name, *columns):
        self.name = name
        self.columns = FieldsList(columns)

    def sql(self, **kwargs):
        return f'{self.name} ({self.columns.sql(**kwargs)})'


class Table:

    def __init__(self, name, columns):
        self.name = name
        self.columns = columns.set_table(self)
        self.constraints = []
        self.options = []

    def primary_key(self, *columns):
        self.constraints.append(
            TableConstraint('PRIMARY KEY', *columns)
        )
        return self

    def unique(self, *columns):
        self.constraints.append(
            TableConstraint('UNIQUE', *columns)
        )
        return self

    # TODO: foreign_key(self, ...):

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
        self.columns = FieldsList(columns)

    def create(self, if_not_exists=False):
        return queries.CreateIndex(self, if_not_exists)

    # TODO: def drop(self, ...):

    def __str__(self):
        return self.name

    # TODO: def __repr__(self):

