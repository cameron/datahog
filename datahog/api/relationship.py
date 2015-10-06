# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

from __future__ import absolute_import

from .. import error
from ..const import table, util
from ..db import query, txn


__all__ = ['create', 'list', 'get', 'set_flags', 'shift', 'remove']


def create(pool, ctx, base_id, rel_id, base_ctx=None, rel_ctx=None, forward_index=None, reverse_index=None, flags=None, timeout=None):
    '''make a new relationship between two id objects

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int ctx: the context for the relationship

    :param int base_id: the id of the first related object

    :param int rel_id: the id of the other related object

    :param int base_ctx: for union rels, the ctx of the first related object

    :param int rel_ctx: for union rels, the ctx of the other related object

    :param int forward_index:
        insert the new forward relationship into position ``index`` for the
        given ``base_id/ctx``, rather than at the end of the list

    :param int reverse_index:
        insert the new reverse relationship into position ``index`` for the
        given ``rel_id/ctx``, rather than at the end of the list

    :param iterable flags:
        the flags to set on the new relationship (default empty). these will be
        ignored if the relationship already exists (therefore isn't newly
        created by this method)

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a boolean of whether the new relationship was created. it wouldn't be
        created in the case that a relationship with the same
        ``ctx/base_id/rel_id`` already exists.

    :raises ReadOnly: if given a read-only db connection pool

    :raises BadContext:
        if ``ctx`` is not a context associated with ``table.RELATIONSHIP``, or
        it doesn't have both a ``base_ctx`` and a ``rel_ctx`` configured.

    :raises BadFlag:
        if ``flags`` contains something that is not a flag associated with the
        given ``ctx``

    :raises NoObject:
        if either of the objects at ``base_ctx/base_id`` or ``rel_ctx/rel_id``
        don't exist
    '''
    if pool.readonly:
        raise error.ReadOnly()

    ctx_base_ctx = util.ctx_base_ctx(ctx)
    ctx_rel_ctx = util.ctx_rel_ctx(ctx)
    for arg_ctx, ctx_ctx in ((base_ctx, ctx_base_ctx), (rel_ctx, ctx_rel_ctx)):
        if isinstance(ctx_ctx, set) and not arg_ctx:
            raise error.MissingContext('Specify base_ctx and rel_ctx to create a union (ctx: %s) relation.' % ctx)

    base_ctx = base_ctx or ctx_base_ctx
    rel_ctx = rel_ctx or ctx_rel_ctx

    if (util.ctx_tbl(ctx) != table.RELATIONSHIP
            or base_ctx is None
            or rel_ctx is None):
        raise error.BadContext(ctx)

    flags = util.flags_to_int(ctx, flags or [])

    return txn.create_relationship_pair(pool, base_id, base_ctx, rel_id, rel_ctx, ctx,
                                        forward_index, reverse_index, flags, timeout)


def list(pool, id, ctx, forward=True, limit=100, start=0, timeout=None):
    '''list the relationships associated with a id object

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int id: id of the parent object

    :param int ctx: context of the relationships to fetch

    :param bool forward:
        if ``True``, then fetches relationships which have ``id`` as their
        ``base_id``, otherwise ``id`` refers to ``rel_id``

    :param int limit: maximum number of relationships to return

    :param int start:
        an integer representing the index in the list of relationships from
        which to start the results

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        two-tuple with a list of relationship dicts (containing ``ctx``,
        ``base_id``, ``rel_id``, and ``flags`` keys), and an integer position
        that can be used as ``start`` in a subsequent call to page forward from
        after the end of this result list.
    '''
    with pool.get_by_id(id, timeout=timeout) as conn:
        results = query.select_relationships(conn.cursor(), id, ctx, forward, limit, start)

    pos = 0
    for result in results:
        result['flags'] = util.int_to_flags(ctx, result['flags'])
        pos = result.pop('pos') + 1

    return results, pos


def get(pool, ctx, base_id, rel_id, timeout=None):
    '''fetch the relationship between two ids

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int ctx: context of the relationship

    :param int base_id: id of the object at one end

    :param int rel_id: id of the object at the other end

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        a relationship dict (with ``ctx``, ``base_id``, ``rel_id``, and
        ``flags`` keys) or None if there is no such relationship
    '''
    with pool.get_by_id(base_id, timeout=timeout) as conn:
        rels = query.select_relationships(
                conn.cursor(), base_id, ctx, True, 1, 0, rel_id)

    rel = rels[0] if rels else None
    if rel:
        rel['flags'] = util.int_to_flags(ctx, rel['flags'])
        rel.pop('pos')

    return rel


def set_flags(pool, base_id, rel_id, ctx, add, clear, timeout=None):
    '''remove flags from a relationship

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the object at one end

    :param int rel_id: the id of the object at the other end

    :param int ctx: the relationship's context

    :param iterable add: the flags to add

    :param iterable clear: the flags to clear

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        the new set of flags, or None if there is no relationship for the given
        ``base_id/rel_id/ctx``

    :raises ReadOnly: if given a read-only pool

    :raises BadContext:
        if the ``ctx`` is not a registered context for table.RELATIONSHIP

    :raises BadFlag:
        if ``flags`` contains something that is not a registered flag
        associated with ``ctx``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    if util.ctx_tbl(ctx) != table.RELATIONSHIP:
        raise error.BadContext(ctx)

    add = util.flags_to_int(ctx, add)
    clear = util.flags_to_int(ctx, clear)

    result = txn.set_relationship_flags(
            pool, base_id, rel_id, ctx, add, clear, timeout)

    if result is None:
        return None

    return util.int_to_flags(ctx, result)


def shift(pool, base_id, rel_id, ctx, forward, index, timeout=None):
    '''change the ordered position of a relationship

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the base_id parent

    :param int rel_id: the id of the rel_id parent

    :param int ctx: the relationship's context

    :param bool forward:
        whether we are shifting this relationship in the list of ``base_id``'s
        forward relationships (``True``), or ``rel_id``'s outgoing
        relationships (``False``)

    :param int index:
        the position in the appropriate list (see the description of
        ``forward``) to which to move this relationship

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean of whether the shift happened or not. it might not happen if
        there is no relationship for the given ``base_id/rel_id/ctx``

    :raises ReadOnly: if given a read-only ``pool``
    '''
    if pool.readonly:
        raise error.ReadOnly()

    anchor_id = base_id if forward else rel_id

    with pool.get_by_id(anchor_id, timeout=timeout) as conn:
        return query.reorder_relationship(
                conn.cursor(), base_id, rel_id, ctx, forward, index)


def remove(pool, base_id, rel_id, ctx, timeout=None):
    '''remove a relationship

    :param ConnectionPool pool:
        a :class:`ConnectionPool <datahog.dbconn.ConnectionPool>` to use for
        getting a database connection

    :param int base_id: the id of the object at one end

    :param int rel_id: the id of the object at the other end

    :param int ctx: the relationshp's context

    :param timeout:
        maximum time in seconds that the method is allowed to take; the default
        of ``None`` means no limit

    :returns:
        boolean, whether the remove was done or not (it would only fail if
        there is no alias for the given ``base_id/ctx/value``)

    :raises ReadOnly: if given a read-only db connection pool
    '''
    if pool.readonly:
        raise error.ReadOnly()

    return txn.remove_relationship_pair(pool, base_id, rel_id, ctx, timeout)
