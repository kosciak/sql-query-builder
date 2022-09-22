import logging


log = logging.getLogger('drewno.sql.core')


class Order:
    ASC = 'ASC'
    DESC = 'DESC'


class Nulls:
    FIRST = 'FIRST'
    LASR = 'LAST'


class Join:
    NATURAL = 'NATURAL'
    INNER = 'INNER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    FULL = 'FULL'
    LEFT_OUTER = 'LEFT OUTER'
    RIGHT_OUTER = 'RIGHT OUTER'
    FULL_OUTER = 'FULL OUTER'
    CROSS = 'CROSS'

