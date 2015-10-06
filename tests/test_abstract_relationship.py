
import os
import sys
import unittest


sys.path.insert(0, '/Users/cam/src/datahog')
import datahog
from datahog import (
  error,
  pool as dbpool,
  relationship,
  node,
)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# yes, this is an integration test, not a unit test..
pool = dbpool.GreenhouseConnPool({
  'shards': [{
    'shard': 0,
    'count': 4,
    'host': '12.12.12.12',
    'port': '6543',
    'user': 'legalease',
    'password': '',
    'database': 'legalease',
  }],
  'lookup_insertion_plans': [[(0, 1)]],
  'shard_bits': 8,
  'digest_key': 'super secret',
})

pool.start()
if not pool.wait_ready(2.):
  raise Exception("postgres connection timeout")


datahog.set_context(1, datahog.NODE)
datahog.set_context(2, datahog.NODE, {'storage': datahog.storage.INT})
union = set((1,2))
datahog.set_context(3, datahog.RELATIONSHIP, {
  'base_ctx': union, 'rel_ctx': union})
a = node.create(pool, 1, None)['id']
b = node.create(pool, 2, 0)['id']

class RelationshipTests(unittest.TestCase):
    def tearDown(self):
        self.assertEqual(len(pool._conns[0]._data), 4)

    def test_create_and_lookup(self):
      self.assertEqual(relationship.create(pool, 3, a, a, 1, 1), True)
      self.assertEqual(relationship.create(pool, 3, a, b, 1, 2), True)
      rels = relationship.list(pool, a, 3)[0]
      self.assertEqual(rels[0]['base_ctx'], 1)
      self.assertEqual(rels[0]['rel_ctx'], 1)
      self.assertEqual(rels[1]['base_ctx'], 1)
      self.assertEqual(rels[1]['rel_ctx'], 2)

    def test_create_failure_missing_context(self):
      self.assertRaises(error.MissingContext,
                        relationship.create, pool, 3, a, b)
      self.assertRaises(error.MissingContext,
                        relationship.create, pool, 3, a, b, 1)

if __name__ == '__main__':
    unittest.main()
