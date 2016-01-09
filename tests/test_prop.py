# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import os
import sys
import unittest

import datahog
from datahog.const import util
from datahog import error
import mummy
import psycopg2

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import base
from pgmock import *


class PropertyTests(base.TestCase):
    def setUp(self):
        super(PropertyTests, self).setUp()
        datahog.set_context(1, datahog.NODE)
        datahog.set_context(2, datahog.PROPERTY,
                {'base_ctx': 1, 'storage': datahog.storage.INT})

    def test_set_insert(self):
        add_fetch_result([(True, False)])

        self.assertEqual(
                datahog.prop.set(self.p, 1234, 2, 10),
                (True, False))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with existencequery as (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
),
updatequery as (
    update property
    set num=%s, value=null
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and exists (select 1 from existencequery)
    returning 1
),
insertquery as (
    insert into property (base_id, ctx, num, flags)
    select %s, %s, %s, %s
    where
        not exists (select 1 from updatequery)
        and exists (select 1 from existencequery)
    returning 1
)
select
    exists (select 1 from insertquery),
    exists (select 1 from updatequery)
""", (1234, 1, 10, 1234, 2, 1234, 2, 10, 0)),
            FETCH_ONE,
            COMMIT])

    def test_set_update(self):
        add_fetch_result([(False, True)])

        self.assertEqual(
                datahog.prop.set(self.p, 1234, 2, 10),
                (False, True))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with existencequery as (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
),
updatequery as (
    update property
    set num=%s, value=null
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and exists (select 1 from existencequery)
    returning 1
),
insertquery as (
    insert into property (base_id, ctx, num, flags)
    select %s, %s, %s, %s
    where
        not exists (select 1 from updatequery)
        and exists (select 1 from existencequery)
    returning 1
)
select
    exists (select 1 from insertquery),
    exists (select 1 from updatequery)
""", (1234, 1, 10, 1234, 2, 1234, 2, 10, 0)),
            FETCH_ONE,
            COMMIT])

    def test_set_fail_no_obj(self):
        add_fetch_result([(False, False)])

        self.assertRaises(error.NoObject,
                datahog.prop.set, self.p, 1234, 2, 10)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
with existencequery as (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
),
updatequery as (
    update property
    set num=%s, value=null
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and exists (select 1 from existencequery)
    returning 1
),
insertquery as (
    insert into property (base_id, ctx, num, flags)
    select %s, %s, %s, %s
    where
        not exists (select 1 from updatequery)
        and exists (select 1 from existencequery)
    returning 1
)
select
    exists (select 1 from insertquery),
    exists (select 1 from updatequery)
""", (1234, 1, 10, 1234, 2, 1234, 2, 10, 0)),
            FETCH_ONE,
            COMMIT])

    def test_set_race_cond_backup(self):
        def initial_failure():
            query_fail(None)
            return psycopg2.IntegrityError()
        query_fail(initial_failure)
        add_fetch_result([()])

        self.assertEqual(
                datahog.prop.set(self.p, 1234, 2, 10),
                (False, True))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE_FAILURE("""
with existencequery as (
    select 1
    from node
    where
        time_removed is null
        and id=%s
        and ctx=%s
),
updatequery as (
    update property
    set num=%s, value=null
    where
        time_removed is null
        and base_id=%s
        and ctx=%s
        and exists (select 1 from existencequery)
    returning 1
),
insertquery as (
    insert into property (base_id, ctx, num, flags)
    select %s, %s, %s, %s
    where
        not exists (select 1 from updatequery)
        and exists (select 1 from existencequery)
    returning 1
)
select
    exists (select 1 from insertquery),
    exists (select 1 from updatequery)
""", (1234, 1, 10, 1234, 2, 1234, 2, 10, 0)),
            ROLLBACK,
            EXECUTE("""
update property
set num=%s, value=%s
where
    time_removed is null
    and base_id=%s
    and ctx=%s
""", (10, None, 1234, 2)),
            ROWCOUNT,
            COMMIT])

    def test_get_success(self):
        add_fetch_result([(15, 0)])

        self.assertEqual(
                datahog.prop.get(self.p, 1234, 2),
                {'base_id': 1234, 'ctx': 2, 'flags': set([]), 'value': 15})

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select num, flags
from property
where
    time_removed is null
    and base_id=%s
    and ctx=%s
""", (1234, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_get_failure(self):
        add_fetch_result([])

        self.assertEqual(
                datahog.prop.get(self.p, 1234, 2),
                None)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select num, flags
from property
where
    time_removed is null
    and base_id=%s
    and ctx=%s
""", (1234, 2)),
            ROWCOUNT,
            COMMIT])

    def test_get_list_list(self):
        datahog.set_context(3, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.STR})
        datahog.set_context(4, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.STR})
        datahog.set_flag(1, 4)
        datahog.set_flag(2, 4)
        datahog.set_flag(3, 4)

        add_fetch_result([
            (2, 10, None, 0),
            (4, None, "foobar", 5)])

        self.assertEqual(
                datahog.prop.get_list(self.p, 123, [2, 3, 4]),
                [
                    {'base_id': 123, 'ctx': 2, 'flags': set([]), 'value': 10},
                    None,
                    {'base_id': 123, 'ctx': 4, 'flags': set([1, 3]),
                        'value': 'foobar'}
                ])

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select ctx, num, value, flags
from property
where
    time_removed is null
    and base_id=%s
    and ctx in (%s, %s, %s)
""", (123, 2, 3, 4)),
            FETCH_ALL,
            COMMIT])

    def test_get_list_all(self):
        datahog.set_context(3, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.STR})
        datahog.set_context(4, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.STR})
        datahog.set_flag(1, 4)
        datahog.set_flag(2, 4)
        datahog.set_flag(3, 4)

        add_fetch_result([
            (2, 10, None, 0),
            (4, None, "foobar", 5)])

        self.assertEqual(
                sorted(datahog.prop.get_list(self.p, 123),
                    key=lambda d: d['ctx']),
                [
                    {'base_id': 123, 'ctx': 2, 'flags': set([]), 'value': 10},
                    {'base_id': 123, 'ctx': 4, 'flags': set([1, 3]),
                        'value': 'foobar'}
                ])

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
select ctx, num, value, flags
from property
where
    time_removed is null
    and base_id=%s
""", (123,)),
            FETCH_ALL,
            COMMIT])

    def test_increment(self):
        add_fetch_result([(10,)])

        self.assertEqual(
                datahog.prop.increment(self.p, 123, 2),
                10)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set num=num+%s
where
    time_removed is null
    and base_id=%s
    and ctx=%s
returning num
""", (1, 123, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_increment_limit_pos(self):
        add_fetch_result([(20,)])

        self.assertEqual(
                datahog.prop.increment(self.p, 123, 2, 5, 20),
                20)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set num=case
    when (num+%s < %s)
    then num+%s
    else %s
    end
where
    time_removed is null
    and base_id=%s
    and ctx=%s
returning num
""", (5, 20, 5, 20, 123, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_increment_limit_neg(self):
        add_fetch_result([(20,)])

        self.assertEqual(
                datahog.prop.increment(self.p, 123, 2, -5, 0),
                20)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set num=case
    when (num+%s > %s)
    then num+%s
    else %s
    end
where
    time_removed is null
    and base_id=%s
    and ctx=%s
returning num
""", (-5, 0, -5, 0, 123, 2)),
            ROWCOUNT,
            FETCH_ONE,
            COMMIT])

    def test_add_flags(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(7,)])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [1, 3], []),
                set([1, 2, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set flags=flags | %s
where time_removed is null and ctx=%s and base_id=%s
returning flags
""", (5, 2, 123)),
            FETCH_ALL,
            COMMIT])

    def test_add_flags_no_prop(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [1, 3], []),
                None)

    def test_clear_flags(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)

        add_fetch_result([(4,)])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [], [1, 2]),
                set([3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set flags=flags & ~%s
where time_removed is null and ctx=%s and base_id=%s
returning flags
""", (3, 2, 123)),
            FETCH_ALL,
            COMMIT])

    def test_clear_flags_no_prop(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [], [1, 3]),
                None)

    def test_set_flags_add(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([(5,)])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [1, 3], []),
                set([1, 3]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set flags=flags | %s
where
    time_removed is null and ctx=%s and base_id=%s
returning flags
""", (5, 2, 123)),
            FETCH_ALL,
            COMMIT])

    def test_set_flags_clear(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [], [2, 3]),
                set([1]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set flags=flags & ~%s
where
    time_removed is null and ctx=%s and base_id=%s
returning flags
""", (6, 2, 123)),
            FETCH_ALL,
            COMMIT])

    def test_set_flags_both(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([(1,)])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [1], [2, 3]),
                set([1]))

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set flags=(flags & ~%s) | %s
where
    time_removed is null and ctx=%s and base_id=%s
returning flags
""", (6, 1, 2, 123)),
            FETCH_ALL,
            COMMIT])

    def test_set_flags_no_prop(self):
        datahog.set_flag(1, 2)
        datahog.set_flag(2, 2)
        datahog.set_flag(3, 2)
        add_fetch_result([])

        self.assertEqual(
                datahog.prop.set_flags(self.p, 123, 2, [1], [2, 3]),
                None)

    def test_remove_success(self):
        add_fetch_result([None]) # just a rowcount

        self.assertEqual(
            datahog.prop.remove(self.p, 123, 2),
            True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set time_removed=now()
where
    time_removed is null
    and base_id=%s
    and ctx=%s
""", (123, 2)),
            ROWCOUNT,
            COMMIT])

    def test_remove_failure(self):
        add_fetch_result([])

        self.assertEqual(
            datahog.prop.remove(self.p, 123, 2),
            False)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set time_removed=now()
where
    time_removed is null
    and base_id=%s
    and ctx=%s
""", (123, 2)),
            ROWCOUNT,
            COMMIT])

    def test_remove_assert_val(self):
        add_fetch_result([None])

        self.assertEqual(
                datahog.prop.remove(self.p, 123, 2, 15),
                True)

        self.assertEqual(eventlog, [
            GET_CURSOR,
            EXECUTE("""
update property
set time_removed=now()
where
    time_removed is null
    and base_id=%s
    and ctx=%s
    and num=%s
""", (123, 2, 15)),
            ROWCOUNT,
            COMMIT])

    def test_storage_null(self):
        datahog.set_context(3, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.NULL
        })

        self.assertRaises(error.StorageClassError, util.storage_wrap, 3, 0)
        self.assertEqual(util.storage_wrap(3, None), None)
        self.assertEqual(util.storage_unwrap(3, None), None)

    def test_storage_str(self):
        datahog.set_context(4, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.STR
        })

        self.assertRaises(error.StorageClassError, util.storage_wrap, 4, u'x')
        self.assertEqual(
                util.storage_wrap(4, 'test').adapted,
                'test')
        self.assertEqual(
                util.storage_unwrap(4, psycopg2.Binary('testing')),
                'testing')

    def test_storage_utf(self):
        datahog.set_context(5, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.UTF
        })

        self.assertRaises(error.StorageClassError, util.storage_wrap, 5, 'no')
        self.assertEqual(
                util.storage_wrap(5, u'testing').adapted,
                u'testing'.encode('utf8'))

        self.assertEqual(
                util.storage_unwrap(5, psycopg2.Binary('testing')),
                u'testing')

    def test_storage_serial(self):
        datahog.set_context(6, datahog.PROPERTY, {
            'base_ctx': 1, 'storage': datahog.storage.SERIAL,
        })

        self.assertEqual(
                util.storage_wrap(6, ['test', 'path', {10: 0.1}]).adapted,
                mummy.dumps(['test', 'path', {10: 0.1}]))

        self.assertEqual(
                util.storage_unwrap(6, psycopg2.Binary('\x10\x03\x08\x04test\x08\x04path\x13\x01\x02\n\x07?\xb9\x99\x99\x99\x99\x99\x9a')),
                ['test', 'path', {10: 0.1}])


if __name__ == '__main__':
    unittest.main()
