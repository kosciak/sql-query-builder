import logging


log = logging.getLogger('drewno.sql.utils')


def fields(fields):
    return ', '.join(
        f'{field!s}' for field in fields
    )


