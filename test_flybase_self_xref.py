"""Checks flybase_self_xref against a fake Neo4j connection.

python3 test_flybase_self_xref.py   (no Neo4j)
"""
import os, sys, unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import flybase_self_xref as fx


class FakeNC:
    def __init__(self, sites):
        self.sites, self.written = sites, []

    def commit_list(self, statements):
        s = statements[0]
        if s.startswith('MATCH (s:Site)'):
            return [{'data': [{'row': [x]} for x in self.sites]}]
        if 'RETURN count(n)' in s:
            return [{'data': [{'row': [7]}]}]
        self.written.extend(statements)
        return [{'data': []}]


class TestFlyBaseSelfXref(unittest.TestCase):

    def test_points_marker_at_the_site(self):
        nc = FakeNC(['FlyBaseSite'])
        self.assertTrue(fx.normalise_flybase_self_xref(nc, log=lambda *_: None))
        self.assertEqual(2, len(nc.written))
        self.assertIn("SET n.self_xref = ['FlyBaseSite']", nc.written[0])
        self.assertIn("'FlyBase' IN ([] + n.self_xref)", nc.written[0])
        self.assertIn('IS NULL', nc.written[1])
        self.assertNotIn('NOT c.self_xref', nc.written[1])   # null-unsafe form the old rule used

    def test_no_apoc(self):
        for s in fx.marker_statements('FlyBaseSite'):
            self.assertNotIn('apoc.', s)    # the pdb image's apoc.jar cannot load apoc.coll.*

    def test_site_named_flybase_needs_no_migration(self):
        stmts = fx.marker_statements('FlyBase')
        self.assertEqual(1, len(stmts))
        self.assertIn("SET c.self_xref = ['FlyBase']", stmts[0])

    def test_missing_or_ambiguous_site_writes_nothing(self):
        for sites in ([], ['FlyBaseSite', 'FlyBase']):
            nc = FakeNC(sites)
            self.assertFalse(fx.normalise_flybase_self_xref(nc, log=lambda *_: None))
            self.assertEqual([], nc.written)

    def test_refuses_an_odd_site_name(self):
        with self.assertRaises(ValueError):
            fx.marker_statements("x'] SET n.a = ['b")


if __name__ == '__main__':
    unittest.main()
