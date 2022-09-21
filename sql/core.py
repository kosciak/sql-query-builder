import logging


log = logging.getLogger('drewno.sql.core')


def get_name(e, **kwargs):
    if hasattr(e, 'get_name'):
        return e.get_name(**kwargs)
    return e


def sql(e, **kwargs):
    if hasattr(e, 'sql'):
        return e.sql(**kwargs)
    return e


class Order:
    ASC = 'ASC'
    DESC = 'DESC'


class Nulls:
    FIRST = 'FIRST'
    LASR = 'LAST'


class Comparable:

    def __eq__(self, value):
        return Operation('=', self, value)

    def __ne__(self, value):
        return Operation('<>', self, value)

    def __lt__(self, value):
        return Operation('<', self, value)

    def __le__(self, value):
        return Operation('<=', self, value)

    def __gt__(self, value):
        return Operation('>', self, value)

    def __ge__(self, value):
        return Operation('>=', self, value)


class Binary:

    def __and__(self, value):
        return Operation('&', self, value)

    def __or__(self, value):
        return Operation('|', self, value)

    def __xor__(self, value):
        return Operation('^', self, value)

    def __lshift__(self, value):
        return Operation('<<', self, value)

    def __rshift__(self, value):
        return Operation('>>', self, value)

    def __rand__(self, value):
        return Operation('&', value, self)

    def __ror__(self, value):
        return Operation('|', value, self)

    def __rxor__(self, value):
        return Operation('^', value, self)

    def __rlshift__(self, value):
        return Operation('<<', value, self)

    def __rrshift__(self, value):
        return Operation('>>', value, self)

    # TODO: __invert__(self):


class Numerical:

    def __add__(self, value):
        return Operation('+', self, value)

    def __sub__(self, value):
        return Operation('-', self, value)

    def __mul__(self, value):
        return Operation('*', self, value)

    def __truediv__(self, value):
        return Operation('/', self, value)

    def __mod__(self, value):
        return Operation('%', self, value)

    def __radd__(self, value):
        return Operation('+', value, self)

    def __rsub__(self, value):
        return Operation('-', value, self)

    def __rmul__(self, value):
        return Operation('*', value, self)

    def __rtruediv__(self, value):
        return Operation('/', value, self)

    def __rmod__(self, value):
        return Operation('%', value, self)



class Aliasable:

    def alias(self, alias):
        return Alias(self, alias)


class Field(Comparable, Binary, Numerical, Aliasable):

    def __init__(self, name, parent=None):
        self.name = name
        self.parent = parent

    @property
    def qualified_name(self):
        if self.parent:
            return f'{self.parent!s}.{self.name}'
        return self.name

    def get_name(self, qualified=False, **kwargs):
        if qualified:
            return self.qualified_name
        return self.name

    def sql(self, **kwargs):
        return self.get_name(**kwargs)

    def __str__(self):
        return self.sql()


class Alias(Field):

    def __init__(self, field, alias):
        super().__init__(name=alias)
        self.field = field

    def sql(self, **kwargs):
        return f'{get_name(self.field, **kwargs)} AS {self.name!s}'


class Operation(Comparable, Binary, Numerical, Aliasable):

    def __init__(self, operator, left, right):
        self.operator = operator
        self.left = left
        self.right = right

    def sql(self, **kwargs):
        return f'{get_name(self.left, **kwargs)}' \
               f' {self.operator} ' \
               f'{get_name(self.right, **kwargs)}'


class FieldsList(list):

    def sql(self, **kwargs):
        return ', '.join(
            f'{sql(field, **kwargs)}' for field in self
        )


class And(list):

    def sql(self, **kwargs):
        return f'({" AND ".join(sql(condition, **kwargs) for condition in self)})'


class Aggregate(Field):

    ALL_COLUMNS = ['*', ]

    def __init__(self, name, *columns, distinct=False):
        self.name = name
        self.distinct = distinct
        self.columns = FieldsList(columns or self.ALL_COLUMNS)

    def get_name(self, **kwargs):
        return self.sql(**kwargs)

    def sql(self, **kwargs):
        return f'{self.name}(' \
               f'{self.distinct and "DISTINCT " or ""}' \
               f'{self.columns.sql(**kwargs)})'

