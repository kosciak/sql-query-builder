import logging


log = logging.getLogger('drewno.sql.core')


def columns_list(columns):
    column_names = [f'{column!s}' for column in columns]
    return ', '.join(column_names)

