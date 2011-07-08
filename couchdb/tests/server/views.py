# -*- coding: utf-8 -*-
#
import unittest
from couchdb import json
from couchdb.server import compiler
from couchdb.server import exceptions
from couchdb.server import state
from couchdb.server import views

class MapTestCase(unittest.TestCase):

    def setUp(self):
        compiler.context['FatalError'] = exceptions.FatalError
        compiler.context['Error'] = exceptions.Error

    def tearDown(self):
        state.reset()
        compiler.context.clear()

    def test_map_doc(self):
        """should apply map function to document"""
        state.add_fun(
            'def mapfun(doc):\n'
            '  yield doc["_id"], "bar"'
        )
        result = views.map_doc({'_id': 'foo'})
        self.assertEqual(result, [[['foo', 'bar']]])

    def test_map_doc_by_many_functions(self):
        """should apply multiple map functions to single document"""
        state.add_fun(
            'def mapfun(doc):\n'
            '  yield doc["_id"], "foo"\n'
            '  yield doc["_id"], "bar"'
        )
        state.add_fun(
            'def mapfun(doc):\n'
            '  yield doc["_id"], "baz"'
        )
        state.add_fun(
            'def mapfun(doc):\n'
            '  return [[doc["_id"], "boo"]]'
        )
        result = views.map_doc({'_id': 'foo'})
        self.assertEqual(result, [[['foo', 'foo'], ['foo', 'bar']],
                                  [['foo', 'baz']], [['foo', 'boo']]])

    def test_rethrow_viewserver_exception_as_is(self):
        """should rethrow any QS exception as is"""
        state.add_fun(
            'def mapfun(doc):\n'
            '  raise FatalError("let it crush!")'
        )
        self.assertRaises(exceptions.FatalError, views.map_doc, {'_id': 'foo'})

    def test_raise_error_exception_on_any_python_one(self):
        """should raise QS Error exception on any Python one"""
        state.add_fun(
            'def mapfun(doc):\n'
            '  1/0'
        )
        try:
            views.map_doc({'_id': 'foo'})
        except Exception, err:
            self.assertTrue(err, exceptions.Error)
            self.assertEqual(err.args[0], ZeroDivisionError.__name__)

    def test_map_function_shouldnt_change_document(self):
        """should prevent document changing within map function"""
        state.add_fun(
            'def mapfun(doc):\n'
            '  assert "bar" not in doc\n'
            '  doc["bar"] = "baz"\n'
            '  yield doc["bar"], 1'
        )
        state.add_fun(
            'def mapfun(doc):\n'
            '  assert "bar" not in doc\n'
            '  yield doc["_id"], 0'
        )
        doc = {'_id': 'foo'}
        views.map_doc(doc)


class ReduceTestCase(unittest.TestCase):

    def setUp(self):
        compiler.context['FatalError'] = exceptions.FatalError
        compiler.context['Error'] = exceptions.Error

    def tearDown(self):
        state.reset()
        compiler.context.clear()

    def test_reduce(self):
        """should reduce map function result"""
        state.add_fun(
            'def mapfun(doc):\n'
            '  return ([doc["_id"], i] for i in range(10))'
        )
        result = views.map_doc({'_id': 'foo'})
        rresult = views.reduce(
            ['def reducefun(keys, values): return sum(values)'],
            result[0]
        )
        self.assertEqual(rresult, [True, [45]])

    def test_reduce_by_many_functions(self):
        """should proceed map keys-values result by multiple reduce functions"""
        state.add_fun(
            'def mapfun(doc):\n'
            '  return ([doc["_id"], i] for i in range(10))'
        )
        result = views.map_doc({'_id': 'foo'})
        rresult = views.reduce(
            ['def reducefun(keys, values): return sum(values)',
             'def reducefun(keys, values): return max(values)',
             'def reducefun(keys, values): return min(values)',],
            result[0]
        )
        self.assertEqual(rresult, [True, [45, 9, 0]])

    def test_fail_if_reduce_output_too_large(self):
        """should fail if reduce output length is greater than 200 chars
        and twice longer than initial data."""
        state.reset({'reduce_limit': True})
        state.add_fun(
            'def mapfun(doc):\n'
            '  return ([doc["_id"], i] for i in range(10))'
        )
        result = views.map_doc({'_id': 'foo'})
        state.line_length = len(json.encode(result))

        try:
            views.reduce(
                ['def reducefun(keys, values): return "-" * 200'],
                result[0]
            )
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.Error))
            self.assertEqual(err.args[0], 'reduce_overflow_error')
        else:
            self.fail('Error exception expected')
        state.line_length = 0

    def test_rethrow_viewserver_exception_as_is(self):
        """should rethrow any QS exception as is"""
        self.assertRaises(
            exceptions.FatalError,
            views.reduce,
            ['def reducefun(keys, values):\n'
            '  raise FatalError("let it crush!")'],
            [['foo', 'bar'], ['bar', 'baz']]
        )

    def test_raise_error_exception_on_any_python_one(self):
        """should raise QS Error exception on any Python one"""
        try:
            views.reduce(
                ['def reducefun(keys, values): return foo'],
                [['foo', 'bar'], ['bar', 'baz']]
            )
        except Exception, err:
            self.assertTrue(err, exceptions.Error)
            self.assertEqual(err.args[0], NameError.__name__)

    def test_reduce_empty_map_result(self):
        """should not fall on empty map result as issue #163 described"""
        res = views.reduce(
            ['def reducefun(keys, values): return sum(values)'],
            []
        )
        self.assertEqual(res, [True, [0]])

    def test_rereduce(self):
        """should rereduce values"""
        res = views.rereduce(
            ['def reducefun(keys, values): return sum(values)'],
            [1, 2, 3, 4, 5]
        )
        self.assertEqual(res, [True, [15]])


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(MapTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ReduceTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
