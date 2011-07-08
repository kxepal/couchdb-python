# -*- coding: utf-8 -*-
#
import unittest
from couchdb.server import filters
from couchdb.server import state

class FiltersTestCase(unittest.TestCase):

    def test_filter(self):
        """should filter documents, returning True for good and False for bad"""
        state.add_fun(
            'def filterfun(doc, req, userctx):\n'
            '  return doc["good"]'
        )
        res = filters.filter(
            [{'foo': 'bar', 'good': True}, {'bar': 'baz', 'good': False}],
            {}, {}
        )
        self.assertEqual(res, [True, [True, False]])
        state.reset()

    def test_ddoc_filter(self):
        """should filter documents using ddoc filter function for 0.11.0+"""
        state.version = (0, 11, 0)
        def filterfun(doc, req, userctx):
            return doc["good"]

        res = filters.ddoc_filter(
            filterfun,
            [{'foo': 'bar', 'good': True}, {'bar': 'baz', 'good': False}],
            {}, {}
        )
        self.assertEqual(res, [True, [True, False]])
        state.version = None

    def test_new_ddoc_filter(self):
        """shouldn't pass userctx argument to filter function since 0.11.1"""
        state.version = (0, 11, 1)
        def filterfun(doc, req):
            return doc["good"]

        res = filters.ddoc_filter(
            filterfun,
            [{'foo': 'bar', 'good': True}, {'bar': 'baz', 'good': False}],
            {}
        )
        self.assertEqual(res, [True, [True, False]])
        state.version = None

    def test_view_filter(self):
        """should use map function as filter"""
        def mapfun(doc):
            if doc['good']:
                yield None, doc
        res = filters.ddoc_views(
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
