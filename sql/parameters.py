import logging

from .core import Field


log = logging.getLogger('sql.parameters')


class Parameter:

    def __init__(self):
        self.count = 0
        self.names = set()

    def __call__(self, name):
        self.count += 1
        self.names.add(name)


class QmarkParameter(Parameter):

    def __call__(self, name=None):
        super().__call__(name)
        return Field('?')


class NumericParameter(Parameter):

    def __call__(self, name):
        super().__call__(name)
        return Field(f':{self.count}')


class NamedParameter(Parameter):

    def __call__(self, name):
        super().__call__(name)
        return Field(f':{name}')


class FormatParameter(Parameter):

    def __call__(self, name=None):
        super().__call__(name)
        return Field('%s')


class PyformatParameter(Parameter):

    def __call__(self, name):
        super().__call__(name)
        return Field(f'%({name})s')


PARAMSTYLES = {
    'qmark': QmarkParameter,
    'numeric': NumericParameter,
    'named': NamedParameter,
    'format': FormatParameter,
    'pyformat': PyformatParameter,
}

def get_parameters_builder(paramstyle):
    return PARAMSTYLES.get(paramstyle)

