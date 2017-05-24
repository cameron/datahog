# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4



import contextlib
import hashlib
import hmac
import random
import sys
import time

import psycopg2
import psycopg2.extensions

from . import query
from .. import error
from ..const import search, table, util


class TwoPhaseCommit(object):
    def __init__(self, pool, shard, name, uniq_data):
        self._pool = pool
        self._shard = shard
        self._name = name
        self._uniq_data = uniq_data
        self._conn = None
        self._failed = False

    def _free_conn(self):
        self._pool.put(self._conn)
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            self._conn = self._pool.get_by_shard(
                    self._shard, replace=False)

        return self._conn

    def rollback(self):
        conn = self._get_conn()
        try:
            conn.tpc_rollback(self._xid)

        except Exception:
            conn.reset()
            raise

        finally:
            self._free_conn()

    def commit(self):
        conn = self._get_conn()
        try:
            conn.tpc_commit(self._xid)

        except Exception:
            conn.reset()
            raise

        finally:
            self._free_conn()

    def fail(self):
        self._failed = True

    def __enter__(self):
        intxn = False
        conn = self._get_conn()

        xid = []
        for ud in self._uniq_data:
            xid.append(str(ud))
        xid = conn.xid(random.randrange(1<<31), self._name, '-'.join(xid)[:64])
        self._xid = xid
        conn.tpc_begin(xid)

        return conn

    def __exit__(self, klass=None, exc=None, tb=None):
        try:
            if self._failed or exc is not None:
                self._conn.tpc_rollback()
                self._failed = True
            else:
                self._conn.tpc_prepare()
                self._conn.reset()

        finally:
            self._conn = None

    @contextlib.contextmanager
    def elsewhere(self):
        if self._failed:
            raise RuntimeError("TPC already failed")

        try:
            yield

        except Exception:
            exc, klass, tb = sys.exc_info()
            try:
                self.rollback()
            except Exception:
                pass

            raise exc(klass).with_traceback(tb)

        else:
            if self._failed:
                self.rollback()
            else:
                self.commit()


class Timer(object):
    def __init__(self, pool, timeout, conn):
        self.pool = pool
        self.timeout = timeout
        self.conn = conn

    def __enter__(self):
        self.t = self.pool._timer(self.timeout, self.ding)
        self.t.start()
        return self

    def __exit__(self, klass=None, exc=None, tb=None):
        if klass is psycopg2.extensions.QueryCanceledError:
            raise error.Timeout()
        self.t.cancel()

    def ding(self):
        if self.conn is not None:
            self.conn.cancel()


def set_property(conn, base_id, ctx, value, flags):
    cursor = conn.cursor()
    try:
        result = query.upsert_property(cursor, base_id, ctx, value, flags)
        return result

    except psycopg2.IntegrityError:
        conn.rollback()
        updated = query.update_property(cursor, base_id, ctx, value)
        return False, bool(updated)


def lookup_alias(pool, digest, ctx, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _lookup_alias(pool, digest, ctx, timer)
    with timer:
        return _lookup_alias(pool, digest, ctx, timer)

def _lookup_alias(pool, digest, ctx, timer):
    for shard in pool.shards_for_lookup_hash(digest):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn

            alias = query.select_alias_lookup(conn.cursor(), digest, ctx)
            if alias is not None:
                return alias

            timer.conn = None

    return None


def set_alias(pool, base_id, ctx, alias, flags, index, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _set_alias(pool, base_id, ctx, alias, flags, index, timer)
    with timer:
        return _set_alias(pool, base_id, ctx, alias, flags, index, timer)

def _set_alias(pool, base_id, ctx, alias, flags, index, timer):
    digest = hmac.new(pool.digestkey, alias.encode('utf8'),
            hashlib.sha1).digest()
    import base64
    digest_b64 = base64.b64encode(digest).strip()

    # look up pre-existing aliases on any but the current insert shard
    insert_shard = pool.shard_for_alias_write(digest)
    owner = None
    for shard in pool.shards_for_lookup_hash(digest):
        if shard == insert_shard:
            continue

        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
            finally:
                timer.conn = None
        del conn

        if owner is not None:
            break

    if owner is not None:
        if owner['base_id'] == base_id:
            return False

        raise error.AliasInUse(alias, ctx)

    tpc = TwoPhaseCommit(pool, insert_shard, 'set_alias',
            (base_id, ctx, digest_b64))
    conn = None
    try:
        with tpc as conn:
            timer.conn = conn
            inserted, owner_id = query.maybe_insert_alias_lookup(
                    conn.cursor(), digest, ctx, base_id, flags)

            if not inserted:
                tpc.fail()

                if owner_id == base_id:
                    return False

                raise error.AliasInUse(alias, ctx)

    except psycopg2.IntegrityError:
        owner = query.select_alias_lookup(conn.cursor(), digest, ctx)

        conn.rollback()

        if owner['base_id'] == base_id:
            return False

        raise error.AliasInUse(alias, ctx)

    finally:
        if conn is not None:
            pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        with pool.get_by_id(base_id) as conn:
            timer.conn = conn
            try:
                result = query.insert_alias(
                        conn.cursor(), base_id, ctx, alias, index, flags)
            finally:
                timer.conn = None

            if not result:
                conn.rollback()
                tpc.fail()
                base_ctx = util.ctx_base_ctx(ctx)
                base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
                raise error.NoObject("%s<%d/%d>" %
                        (base_tbl, base_ctx, base_id))

    return True


def set_alias_flags(pool, base_id, ctx, alias, add, clear, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _set_alias_flags(pool, base_id, ctx, alias, add, clear, timer)
    with timer:
        return _set_alias_flags(pool, base_id, ctx, alias, add, clear, timer)

def _set_alias_flags(pool, base_id, ctx, alias, add, clear, timer):
    digest = hmac.new(pool.digestkey, alias.encode('utf8'),
            hashlib.sha1).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_lookup_hash(digest):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
            finally:
                timer.conn = None

            if owner is None:
                continue

            if owner['base_id'] != base_id:
                return None

            lookup_shard = shard
            break
    else:
        return None

    tpc = TwoPhaseCommit(pool, lookup_shard, 'set_alias_flags',
            (base_id, ctx, digest_b64, add, clear))
    try:
        with tpc as conn:
            cursor = conn.cursor()
            timer.conn = conn
            try:
                result = query.set_flags(cursor, 'alias_lookup', add, clear,
                        {'hash': digest, 'ctx': ctx})
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return None
    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_id(base_id) as conn:
            timer.conn = conn
            try:
                result = query.set_flags(conn.cursor(), 'alias', add, clear,
                        {'base_id': base_id, 'ctx': ctx, 'value': alias})
            finally:
                timer.conn = None

            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def remove_alias(pool, base_id, ctx, alias, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_alias(pool, base_id, ctx, alias, timer)
    with timer:
        return _remove_alias(pool, base_id, ctx, alias, timer)

def _remove_alias(pool, base_id, ctx, alias, timer):
    digest = hmac.new(pool.digestkey, alias.encode('utf8'),
            hashlib.sha1).digest()
    digest_b64 = digest.encode('base64').strip()

    for shard in pool.shards_for_lookup_hash(digest):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                owner = query.select_alias_lookup(conn.cursor(), digest, ctx)
            finally:
                timer.conn = None

            if owner is None:
                continue

            if owner['base_id'] != base_id:
                return False

            lookup_shard = shard
            break
    else:
        return False

    tpc = TwoPhaseCommit(
            pool, lookup_shard, 'remove_alias', (base_id, ctx, digest_b64))
    try:
        with tpc as conn:
            cursor = conn.cursor()
            timer.conn = conn
            try:
                result = query.remove_alias_lookup(
                        cursor, digest, ctx, base_id)
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return False
    finally:
        pool.put(conn)

    with tpc.elsewhere():
        with pool.get_by_id(base_id) as conn:
            timer.conn = conn
            try:
                result = query.remove_alias(conn.cursor(), base_id, ctx, alias)
            finally:
                timer.conn = None

            if not result:
                conn.rollback()
                tpc.fail()
                return False

    return True


def create_relationship_pair(
        pool, base_id, rel_id, ctx, value, forw_idx, rev_idx, flags, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _create_relationship_pair(
            pool, base_id, rel_id, ctx, value, forw_idx, rev_idx, flags, timer)
    with timer:
        return _create_relationship_pair(
            pool, base_id, rel_id, ctx, value, forw_idx, rev_idx, flags, timer)

def _create_relationship_pair(
        pool, base_id, rel_id, ctx, value, forw_idx, rev_idx, flags, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id),
            'create_relationship_pair', (base_id, rel_id, ctx))
    conn = None
    try:
        with tpc as conn:
            timer.conn = conn
            try:
                inserted = query.insert_relationship(
                    conn.cursor(), base_id, rel_id, ctx, value, True, forw_idx, flags)
            finally:
                timer.conn = None

            if not inserted:
                tpc.fail()

                base_ctx = util.ctx_base_ctx(ctx)
                base_tbl = table.NAMES[util.ctx_tbl(base_ctx)]
                raise error.NoObject("%s<%d/%d>" %
                        (base_tbl, base_ctx, base_id))

    except psycopg2.IntegrityError:
        return False

    finally:
        if conn:# TODO should conn ever be none? is tpc not working?
            pool.put(conn)

    try:
        with tpc.elsewhere():
            with pool.get_by_id(rel_id) as conn:
                timer.conn = conn
                try:
                    inserted = query.insert_relationship(
                        conn.cursor(), base_id, rel_id, ctx, value, False, rev_idx, flags)
                finally:
                    timer.conn = None

                if not inserted:
                    tpc.fail()

                    rel_ctx = util.ctx_rel_ctx(ctx)
                    rel_tbl = table.NAMES[util.ctx_tbl(rel_ctx)]
                    raise error.NoObject("%s<%d/%d>" %
                            (rel_tbl, rel_ctx, rel_id))

    except psycopg2.IntegrityError:
        return False

    return True


def update_relationship(pool, base_id, rel_id, ctx, value, old_value, forward, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _update_relationship(
                pool, base_id, rel_id, ctx, value, old_value, forward, timer)
    with timer:
        return _update_relationship(
                pool, base_id, rel_id, ctx, value, old_value, forward, timer)


def _update_relationship(pool, base_id, rel_id, ctx, value, old_value, forward, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id),
            'update_relationship', (base_id, rel_id, ctx, value, old_value, forward))

    # hacks for undirected rels
    directed = util.ctx_directed(ctx)

    try:
        with tpc as conn:
            timer.conn = conn
            try:
                result = query.update_relationship(
                    conn.cursor(), base_id, rel_id, ctx, value, old_value, forward)
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)

    first_result = result

    with tpc.elsewhere():
        with pool.get_by_id(rel_id) as conn:
            timer.conn = conn
            try:
                ids = (base_id, rel_id)
                forward = False 
                if not directed:
                    ids = (rel_id, base_id)
                    forward = True
                result = query.update_relationship(
                        conn.cursor(), ids[0], ids[1], ctx, value, old_value, forward)
            finally:
                timer.conn = None

            if not result or result != first_result:
                conn.rollback()
                tpc.fail()
                return None

    return True


def set_relationship_flags(pool, base_id, rel_id, ctx, add, clear, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _set_relationship_flags(
                pool, base_id, rel_id, ctx, add, clear, timer)
    with timer:
        return _set_relationship_flags(
                pool, base_id, rel_id, ctx, add, clear, timer)

def _set_relationship_flags(pool, base_id, rel_id, ctx, add, clear, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id),
            'set_relationship_flags', (base_id, rel_id, ctx, add, clear))

    # hacks for undirected rels
    directed = util.ctx_directed(ctx)

    try:
        with tpc as conn:
            timer.conn = conn
            try:
                result = query.set_flags(
                        conn.cursor(), 'relationship', add, clear,
                        {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                            'forward': True})
            finally:
                timer.conn = None

            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)

    result_flags = result[0]

    with tpc.elsewhere():
        with pool.get_by_id(rel_id) as conn:
            timer.conn = conn
            try:
                where = {'base_id': base_id, 'rel_id': rel_id, 'ctx': ctx,
                         'forward': False }
                if not directed:
                    where = {'base_id': rel_id, 'rel_id': base_id, 'ctx': ctx,
                             'forward': True }
                result = query.set_flags(
                        conn.cursor(), 'relationship', add, clear, where)
            finally:
                timer.conn = None

            if not result or result[0] != result_flags:
                conn.rollback()
                tpc.fail()
                return None

    return result_flags


def remove_relationship_pair(pool, base_id, rel_id, ctx, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_relationship_pair(pool, base_id, rel_id, ctx, timer)
    with timer:
        return _remove_relationship_pair(pool, base_id, rel_id, ctx, timer)

def _remove_relationship_pair(pool, base_id, rel_id, ctx, timer):
    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id),
            'remove_relationship_pair', (base_id, rel_id, ctx))
    try:
        with tpc as conn:
            timer.conn = conn
            try:
                removed = query.remove_relationship(
                        conn.cursor(), base_id, rel_id, ctx, True)
            finally:
                timer.conn = None

            if not removed:
                tpc.fail()
                return False
    finally:
        pool.put(conn)

    with tpc.elsewhere():
        conn = pool.get_by_id(rel_id, replace=False)
        timer.conn = conn
        # manually managing commits/rollbacks and replacing on the pool
        # so we don't get an extra COMMIT when we just ROLLBACKed
        try:
            removed = query.remove_relationship(
                    conn.cursor(), base_id, rel_id, ctx, False)
        except Exception:
            conn.rollback()
            tpc.fail()
            return False
        else:
            if removed:
                conn.commit()
            else:
                conn.rollback()
                tpc.fail()
            return removed
        finally:
            pool.put(conn)


def create_node(pool, base_id, ctx, value, index, flags, timeout):
    if base_id is None:
        shard = pool.shard_for_root_insert()
    else:
        shard = pool.shard_by_id(base_id)

    with pool.get_by_shard(shard, timeout=timeout) as conn:
        cursor = conn.cursor()
        node = query.insert_node(cursor, base_id, ctx, value, flags)

        if node is None:
            return None

        if base_id is not None:
            query.insert_edge(cursor, base_id, ctx, node['id'], index, False)

        return node


def move_node(pool, node_id, ctx, base_id, new_base_id, index, timeout):
    if pool.shard_by_id(base_id) == pool.shard_by_id(new_base_id):
        with pool.get_by_id(base_id, timeout=timeout) as conn:
            cursor = conn.cursor()
            if not query.remove_edge(cursor, base_id, ctx, node_id):
                return False

            base_ctx = util.ctx_base_ctx(ctx)
            if not query.insert_edge(
                    cursor, new_base_id, ctx, node_id, index, True):
                conn.rollback()
                return False

        return True

    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _move_node(pool, node_id, ctx, base_id, new_base_id, timer)
    with timer:
        return _move_node(pool, node_id, ctx, base_id, new_base_id, timer)


def _move_node(pool, node_id, ctx, base_id, new_base_id, timer):
    base_ctx = util.ctx_base_ctx(ctx)

    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id), 'move_node',
            (node_id, ctx, base_id, new_base_id))
    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_edge(
                    conn.cursor(), base_id, ctx, node_id):
                tpc.fail()
                return False
    finally:
        pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        with pool.get_by_id(new_base_id) as conn:
            timer.conn = conn
            try:
                if not query.insert_edge(conn.cursor(),
                        new_base_id, ctx, node_id, None, base_ctx):
                    tpc.fail()
                    return False
            finally:
                timer.conn = None

    return True


def create_name(pool, base_id, ctx, value, flags, index, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _create_name(pool, base_id, ctx, value, flags, index, timer)
    with timer:
        return _create_name(pool, base_id, ctx, value, flags, index, timer)

def _create_name(pool, base_id, ctx, value, flags, index, timer):
    base_ctx = util.ctx_base_ctx(ctx)

    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id), 'create_name',
            (base_id, ctx, value.encode('ascii', 'ignore'), flags, index))
    conn = None
    try:
        with tpc as conn:
            timer.conn = conn
            inserted = query.insert_name(
                    conn.cursor(), base_id, ctx, value, flags, index)

            if not inserted:
                tpc.fail()
                return False

    except psycopg2.IntegrityError:
        conn.rollback()
        return False

    finally:
        if conn is not None:
            pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        if not _write_name_lookup(
                pool, tpc, base_id, ctx, value, flags, timer):
            tpc.fail()
            return False

    return True


def _write_name_lookup(pool, tpc, base_id, ctx, value, flags, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _write_prefix_lookup(pool, base_id, ctx, value, flags, timer)

    if sclass == search.PHONETIC:
        return _write_phonetic_lookups(pool, base_id, ctx, value, flags, timer)

    if sclass is None:
        raise error.BadContext(ctx)


def _write_prefix_lookup(pool, base_id, ctx, value, flags, timer):
    with pool.get_by_shard(
            pool.shard_for_prefix_write(value.encode('utf8'))) as conn:
        timer.conn = conn
        try:
            return query.insert_prefix_lookup(
                    conn.cursor(), value, flags, ctx, base_id)
        finally:
            timer.conn = None


def _write_phonetic_lookups(pool, base_id, ctx, value, flags, timer):
    dm, dmalt = util.dmetaphone(value)
    shard1 = pool.shard_for_phonetic_write(dm)
    tpc = TwoPhaseCommit(pool, shard1, 'phonetic_lookup_writes',
            (base_id, ctx, value.encode('ascii', 'ignore'), flags, shard1))

    try:
        with tpc as conn:
            timer.conn = conn
            inserted = query.insert_phonetic_lookup(
                    conn.cursor(), value, dm, flags, ctx, base_id)
    finally:
        timer.conn = None
        pool.put(conn)

    if not inserted:
        tpc.rollback()
        return False

    if dmalt is None or not util.ctx_phonetic_loose(ctx):
        tpc.commit()
        return True

    with tpc.elsewhere():
        shard2 = pool.shard_for_phonetic_write(dmalt)
        with pool.get_by_shard(shard2) as conn:
            timer.conn = conn
            try:
                inserted = query.insert_phonetic_lookup(
                        conn.cursor(), value, dmalt, flags, ctx, base_id)
            finally:
                timer.conn = None

            if not inserted:
                conn.rollback()
                tpc.fail()

    return inserted


def search_names(pool, value, ctx, limit, start, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _search_names(pool, value, ctx, limit, start, timer)
    with timer:
        return _search_names(pool, value, ctx, limit, start, timer)


def _search_names(pool, value, ctx, limit, start, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _search_prefix(pool, value, ctx, limit, start, timer)

    if sclass == search.PHONETIC:
        return _search_phonetic(pool, value, ctx, limit, start, timer)


def _search_prefix(pool, value, ctx, limit, start, timer):
    if start is None:
        start = ''

    names = []
    shards = list(pool.shards_for_lookup_prefix(value.encode('utf8')))
    for shard in shards:
        with pool.get_by_shard(shard) as conn:
            try:
                timer.conn = conn
                names.extend(query.search_prefixes(
                    conn.cursor(), value, ctx, limit, start))
            finally:
                timer.conn = None

    if len(shards) > 1:
        names.sort(key=lambda name: name['value'])
        names = names[:limit]

    return names, names[-1]['value']


def _sortkey(shardbits):
    def f(d):
        return (d['base_id'] & ((1 << (64 - shardbits)) - 1)), d['base_id']
    return f

def _phontoken(results):
    token = {}
    for r in results:
        token[r.pop('code')] = r['base_id']
    return token

def _search_phonetic(pool, value, ctx, limit, start, timer):
    if start is None:
        start = {}

    dm, dmalt = util.dmetaphone(value)
    results = []
    for shard in pool.shards_for_lookup_phonetic(dm):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                results.extend(query.search_phonetics(
                        conn.cursor(), dm, ctx, limit, start.get(dm, 0)))
            finally:
                timer.conn = None

    if dmalt is None or not util.ctx_phonetic_loose(ctx):
        return results, _phontoken(results)

    for shard in pool.shards_for_lookup_phonetic(dmalt):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                results.extend(query.search_phonetics(
                    conn.cursor(), dmalt, ctx, limit, start.get(dmalt, 0)))
            finally:
                timer.conn = None

    # global sort
    results.sort(key=_sortkey(pool.shardbits))

    token = _phontoken(results)

    # de-duplicate by the unique criteria
    copy = []
    seen = set()
    for r in results:
        trip = (r['base_id'], r['ctx'], r['value'])
        if trip in seen:
            continue
        seen.add(trip)
        copy.append(r)
    results = copy
    return results[:limit], token


def set_name_flags(pool, base_id, ctx, value, add, clear, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _set_name_flags(pool, base_id, ctx, value, add, clear, timer)
    with timer:
        return _set_name_flags(pool, base_id, ctx, value, add, clear, timer)

def _set_name_flags(pool, base_id, ctx, value, add, clear, timer):
    lookup_shard = _find_name_lookup_shard(pool, base_id, ctx,
            value.encode('utf8'), timer)
    if lookup_shard is None:
        return None

    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id), 'set_name_flags',
            (base_id, ctx, value.encode('ascii', 'ignore'), add, clear))

    try:
        with tpc as conn:
            timer.conn = conn
            result = query.set_flags(conn.cursor(), 'name', add, clear,
                    {'base_id': base_id, 'ctx': ctx, 'value': value})
            if not result:
                tpc.fail()
                return None

    finally:
        pool.put(conn)
        timer.conn = None

    result_flags = result[0]

    with tpc.elsewhere():
        sclass = util.ctx_search(ctx)
        if sclass == search.PREFIX:
            if not _apply_flags_to_prefix_lookup(pool, lookup_shard,
                    add, clear, base_id, ctx, value, timer, result_flags):
                return None
        elif sclass == search.PHONETIC:
            if not _apply_flags_to_phonetic_lookups(pool, lookup_shard,
                    add, clear, base_id, ctx, value, timer, result_flags):
                return None
        else:
            raise error.BadContext(ctx)

    return result_flags


def _find_name_lookup_shard(pool, base_id, ctx, value, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _find_prefix_lookup_shard(pool, base_id, ctx, value, timer)

    if sclass == search.PHONETIC:
        return _find_phonetic_lookup_shards(pool, base_id, ctx, value, timer)

    raise error.BadContext(ctx)


def _find_prefix_lookup_shard(pool, base_id, ctx, value, timer):
    for shard in pool.shards_for_lookup_prefix(value):
        with pool.get_by_shard(shard) as conn:
            try:
                timer.conn = conn
                if query.select_prefix_lookups(
                        conn.cursor(), value, ctx, base_id):
                    return shard

            finally:
                timer.conn = None

    return None


def _find_phonetic_lookup_shards(pool, base_id, ctx, value, timer):
    dm, dmalt = util.dmetaphone(value)

    for shard in pool.shards_for_lookup_phonetic(dm):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                if query.find_phonetic_lookup(
                        conn.cursor(), dm, ctx, value, base_id):
                    dmshard = shard
                    break
            finally:
                timer.conn = None
    else: 
        return None

    if (dmalt is None) or not util.ctx_phonetic_loose:
        return (dmshard, None)

    for shard in pool.shards_for_lookup_phonetic(dmalt):
        with pool.get_by_shard(shard) as conn:
            timer.conn = conn
            try:
                if query.find_phonetic_lookup(
                        conn.cursor(), dmalt, ctx, value, base_id):
                    dmashard = shard
                    break
            finally:
                timer.conn = None
    else:
        return None

    return dmshard, dmashard


def _apply_flags_to_prefix_lookup(
        pool, lookup_shard, add, clear, base_id, ctx, value, timer, expected):
    with pool.get_by_shard(lookup_shard) as conn:
        timer.conn = conn
        try:
            result = query.set_flags(
                    conn.cursor(), 'prefix_lookup', add, clear,
                    {'base_id': base_id, 'ctx': ctx, 'value': value})
        finally:
            timer.conn = None

        if not result or result[0] != expected:
            conn.rollback()
            return False

    return True


def _apply_flags_to_phonetic_lookups(pool, lookup_shard,
        add, clear, base_id, ctx, value, timer, expected):
    dmshard, dmashard = lookup_shard

    if dmashard is not None:
        return _apply_flags_to_phonetic_lookups_both(pool, lookup_shard,
                add, clear, base_id, ctx, value, timer, expected)

    dm, dmalt = util.dmetaphone(value)

    with pool.get_by_shard(dmshard) as conn:
        timer.conn = conn
        try:
            result = query.set_flags(conn.cursor(), 'phonetic_lookup',
                    add, clear, {'ctx': ctx, 'value': value, 'code': dm,
                        'base_id': base_id})
        finally:
            timer.conn = None

        if not result or result[0] != expected:
            conn.rollback()
            return False

    return True


def _apply_flags_to_phonetic_lookups_both(
        pool, lookup_shard, add, clear, base_id, ctx, value, timer, expected):
    dmshard, dmashard = lookup_shard
    dm, dmalt = util.dmetaphone(value)
    tpc = TwoPhaseCommit(pool, dmshard, 'apply_flag_phonetic',
            (base_id, ctx, add, clear))
    conn = None
    try:
        with tpc as conn:
            timer.conn = conn
            result = query.set_flags(conn.cursor(), 'phonetic_lookup',
                    add, clear, {'ctx': ctx, 'value': value, 'code': dm,
                        'base_id': base_id})

            if not result or result[0] != expected:
                tpc.fail()
                return False
    finally:
        if conn is not None:
            pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        with pool.get_by_shard(dmashard) as conn:
            timer.conn = conn
            try:
                result = query.set_flags(conn.cursor(), 'phonetic_lookup',
                        add, clear, {'ctx': ctx, 'value': value, 'code': dmalt,
                            'base_id': base_id})
            finally:
                timer.conn = None

            if not result or result[0] != expected:
                tpc.fail()
                conn.rollback()
                return False

    return True


def reorder_name(pool, base_id, ctx, value, index, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _reorder_name(pool, base_id, ctx, value, index, timer)
    with timer:
        return _reorder_name(pool, base_id, ctx, value, index, timer)

def _reorder_name(pool, base_id, ctx, value, index, timer):
    conn = pool.get_by_id(base_id, replace=False)
    try:
        result = query.reorder_name(conn.cursor(), base_id, ctx, value, index)
    except Exception:
        conn.rollback()
        raise
    else:
        if result:
            conn.commit()
        else:
            conn.rollback()
    finally:
        pool.put(conn)

    return result


def remove_name(pool, base_id, ctx, value, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_name(pool, base_id, ctx, value, timer)
    with timer:
        return _remove_name(pool, base_id, ctx, value, timer)


def _remove_name(pool, base_id, ctx, value, timer):
    lookup_shard = _find_name_lookup_shard(pool, base_id, ctx,
            value.encode('utf8'), timer)

    tpc = TwoPhaseCommit(pool, pool.shard_by_id(base_id), 'remove_name',
            (base_id, ctx, value.encode('ascii', 'ignore')))

    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_name(conn.cursor(), base_id, ctx, value):
                tpc.fail()
                return False
    finally:
        pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        if not _remove_lookup(pool, lookup_shard, base_id, ctx, value, timer):
            tpc.fail()
            return False

    return True


def _remove_lookup(pool, lookup_shard, base_id, ctx, value, timer):
    sclass = util.ctx_search(ctx)

    if sclass == search.PREFIX:
        return _remove_prefix_lookup(
                pool, lookup_shard, base_id, ctx, value, timer)

    if sclass == search.PHONETIC:
        return _remove_phonetic_lookups(
                pool, lookup_shard, base_id, ctx, value, timer)

    raise error.BadContext(ctx)


def _remove_prefix_lookup(pool, lookup_shard, base_id, ctx, value, timer):
    with pool.get_by_shard(lookup_shard) as conn:
        timer.conn = conn
        try:
            return query.remove_prefix_lookup(
                    conn.cursor(), base_id, ctx, value)
        finally:
            timer.conn = None


def _remove_phonetic_lookups(pool, lookup_shard, base_id, ctx, value, timer):
    dmshard, dmashard = lookup_shard

    if dmashard is not None:
        return _remove_phonetic_lookups_both(
                pool, lookup_shard, base_id, ctx, value, timer)

    dm, dma = util.dmetaphone(value)

    with pool.get_by_shard(dmshard) as conn:
        timer.conn = conn
        try:
            return query.remove_phonetic_lookup(
                    conn.cursor(), base_id, ctx, dm, value)
        finally:
            timer.conn = None

def _remove_phonetic_lookups_both(
        pool, lookup_shard, base_id, ctx, value, timer):
    dmshard, dmashard = lookup_shard
    dm, dma = util.dmetaphone(value)

    tpc = TwoPhaseCommit(pool, dmshard, 'remove_phonetic_lookups',
            (base_id, ctx, value.encode('ascii', 'ignore')))
    conn = None
    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_phonetic_lookup(
                    conn.cursor(), base_id, ctx, dm, value):
                return False
    finally:
        if conn is not None:
            pool.put(conn)
        timer.conn = None

    with tpc.elsewhere():
        conn = pool.get_by_shard(dmashard, replace=False)
        timer.conn = conn
        try:
            if not query.remove_phonetic_lookup(
                    conn.cursor(), base_id, ctx, dma, value):
                tpc.fail()
                conn.rollback()
                return False
            conn.commit()
        finally:
            timer.conn = None
            pool.put(conn)

        return True


def _remove_lookups(cursor, triples):
    prefixes = []
    phonetics = []
    for triple in triples:
        sclass = util.ctx_search(triple[1])
        if sclass == search.PREFIX:
            prefixes.append(triple)
        elif sclass == search.PHONETIC:
            phonetics.append(triple)

    removed = query.remove_prefix_lookups_multi(cursor, prefixes)
    removed.extend(query.remove_phonetic_lookups_multi(cursor, phonetics))

    return removed


def _remove_local_estates(shard, pool, cursor, estate, node_base):
    ids = estate[shard][3][:]
    del estate[shard][3][:]

    while ids:
        if not node_base:
            ids = query.remove_nodes(cursor, ids)
            if not ids:
                break
        node_base = False

        query.remove_properties_multiple_bases(cursor, ids)

        aliases = query.remove_aliases_multiple_bases(cursor, ids)
        for value, ctx in aliases:
            digest = hmac.new(pool.digestkey, value, hashlib.sha1).digest()
            # add each alias_lookup to every shard it *might* live on
            for s in pool.shards_for_lookup_hash(digest):
                group = estate.setdefault(s, (set(), set(), [], []))[0]
                group.add((digest, ctx))

        names = query.remove_names_multiple_bases(cursor, ids)
        for base_id, ctx, value in names:
            for s in pool.shards_for_lookup_prefix(value):
                group = estate.setdefault(s, (set(), set(), [], []))[1]
                group.add((base_id, ctx, value))

        removed_rels = query.remove_relationships_multiple_bases(cursor, ids)
        for base_id, ctx, forward, rel_id in removed_rels:
            # append each relationship to the shard at the rel_id end
            if forward:
                s = pool.shard_by_id(rel_id)
            else:
                s = pool.shard_by_id(base_id)
            if s == shard:
                continue
            item = (base_id, ctx, not forward, rel_id)
            estate.setdefault(s, (set(), set(), [], []))[2].append(item)

        children = query.remove_edges_multiple_bases(cursor, ids)
        for id in children:
            # append each child node to its shard
            s = pool.shard_by_id(id)
            estate.setdefault(s, (set(), set(), [], []))[3].append(id)

        ids = estate[shard][3][:]
        del estate[shard][3][:]

    alias_lookups, name_lookups, rels, ids = estate[shard]

    if alias_lookups:
        removed = query.remove_alias_lookups_multi(cursor, list(alias_lookups))
        for pair in removed:
            for s in pool.shards_for_lookup_hash(pair[0]):
                if s == shard:
                    continue
                estate[s][0].discard(pair)

    if name_lookups:
        removed = _remove_lookups(cursor, name_lookups)
        for triple in removed:
            for s in pool.shards_for_lookup_prefix(triple[2]):
                if s == shard:
                    continue
                estate[s][1].discard(triple)

    if rels:
        query.remove_relationships_multi(cursor, rels)

        forw, rev = set(), set()
        for base_id, ctx, forward, rel_id in rels:
            if forward:
                forw.add((base_id, ctx))
            else:
                rev.add((rel_id, ctx))
        if forw:
            query.bulk_reorder_relationships(cursor, forw, True)
        if rev:
            query.bulk_reorder_relationships(cursor, rev, False)


    estate.pop(shard)


def remove_node(pool, id, ctx, base_id, timeout):
    timer = Timer(pool, timeout, None)
    if timeout is None:
        return _remove_node(pool, id, ctx, base_id, timer)
    with timer:
        return _remove_node(pool, id, ctx, base_id, timer)

def _remove_node(pool, id, ctx, base_id, timer):
    shard = pool.shard_by_id(base_id)
    tpc = TwoPhaseCommit(pool, shard, "remove_node_edge",
            (id, ctx, base_id, shard))
    tpcs = [tpc]

    try:
        with tpc as conn:
            timer.conn = conn
            if not query.remove_edge(
                    conn.cursor(), base_id, ctx, id):
                tpc.fail()
                return False
    finally:
        pool.put(conn)
        timer.conn = None

    estates = {pool.shard_by_id(id): (set(), set(), [], [id])}

    try:
        while estates:
            shard = next(iter(estates))
            tpc = TwoPhaseCommit(pool, shard, 'remove_node_shard',
                    (id, ctx, base_id, shard))
            tpcs.append(tpc)

            try:
                with tpc as conn:
                    _remove_local_estates(next(iter(estates)),
                            pool, conn.cursor(), estates, False)
            finally:
                pool.put(conn)
    except Exception:
        klass, exc, tb = sys.exc_info()
        for tpc in tpcs:
            try:
                tpc.rollback()
            except Exception:
                pass
        raise klass(exc).with_traceback(tb)
    else:
        for tpc in tpcs:
            tpc.commit()

    return True
