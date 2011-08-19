# -*- coding: utf-8 -*-
#
import unittest
from couchdb.server import filters
from couchdb.server import state
from couchdb.server.mock import MockQueryServer

class FiltersTestCase(unittest.TestCase):

    def setUp(self):
        self.server = MockQueryServer()

    def test_filter(self):
        """should filter documents, returning True for good and False for bad"""
        state.add_fun(
            self.server,
            'def filterfun(doc, req, userctx):\n'
            '  return doc["good"]'
        )
        res = filters.filter(
            self.server,
            [{'foo': 'bar', 'good': True}, {'bar': 'baz', 'good': False}],
            {}, {}
        )
        self.assertEqual(res, [True, [True, False]])

    def test_ddoc_filter(self):
        """should filter documents using ddoc filter function for 0.11.0+"""
        server = MockQueryServer((0, 11, 0))
        def filterfun(doc, req, userctx):
            return doc["good"]

        res = filters.ddoc_filter(
            server,
            filterfun,
            [{'foo': 'bar', 'good': True}, {'bar': 'baz', 'good': False}],
            {}, {}
        )
        self.assertEqual(res, [True, [True, False]])

    def test_new_ddoc_filter(self):
        """shouldn't pass userctx argument to filter function since 0.11.1"""
        server = MockQueryServer((0, 11, 1))
        def filterfun(doc, req):
            return doc["good"]

        res = filters.ddoc_filter(
            server,
            filterfun,
            [{'foo': 'bar', 'good': True}, {'bar': 'baz', 'good': False}],
            {}
        )
        self.assertEqual(res, [True, [True, False]])

    def test_view_filter(self):
        """should use map function as filter"""
        def mapfun(doc):
            if doc['good']:
                yield None, doc
        res = filters.ddoc_views(
            self.server,
            mapfun,
            [{'foo': 'bar', 'good': True}, {'bar': 'baz', 'good': False}],
        )
        self.assertEqual(res, [True, [True, False]])

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FiltersTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
