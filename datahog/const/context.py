# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from . import table


META = {}

STORAGE_NULL = 0
STORAGE_INT = 1
STORAGE_STR = 2
STORAGE_UTF8 = 3
STORAGE_SER = 4
STORAGE_TYPES = frozenset([
    STORAGE_NULL,
    STORAGE_INT,
    STORAGE_STR,
    STORAGE_UTF8,
    STORAGE_SER
])


def set_context(title, value, table, meta=None):
    '''create a constant for use in 'ctx'

    :param str title:
        the constant name. will be made available as
        datahog.const.context.NAME.

    :param int value: the integer value to place in the 'ctx' column

    :param int table:
        the table for which this context applies (must be a table from
        datahog.const.table)

    :param dict meta:
        dict for specifying other meta-data about the context.

        possible values:

            base_ctx
                the context value of the object to which it is related through
                its ``base_id``. applies when ``table`` is ``table.TREENODE``,
                ``table.PROPERTY``, ``table.ALIAS``, or
                ``table.RELATIONSHIP``.

            rel_ctx
                the context value of the object to which it is related through
                its ``rel_id``. applies when ``table`` is
                ``table.RELATIONSHIP``.

            storage
                defines behavior of the int/str storage columns. must be one of
                ``STORAGE_NULL``, ``STORAGE_INT``, ``STORAGE_STR``,
                ``STORAGE_SER``. applies when ``table`` is ``table.PROPERTY``
                or ``table.TREENODE``.
    '''
    if title in globals():
        raise ValueError("context name already in use: %s" % title)

    if value in META:
        raise ValueError("duplicate context values: %s, %s" %
                (META[value][0], title))

    if table not in table.REVERSE:
        raise ValueError("unrecognized table const: %r" % table)

    if meta:
        for rel in ('base', 'rel'):
            ctxkey = '%s_ctx' % (rel,)
            if ctxkey not in meta:
                continue

            if meta[ctxkey] not in META:
                raise ValueError("related %s context %d doesn't exist" %
                        (rel, meta[ctxkey]))

        if meta.get('storage', STORAGE_NULL) not in STORAGE_TYPES:
            raise ValueError("unrecognized storage type: %d" % meta['storage'])

    globals()[title] = value
    META[value] = (title, table, meta)
