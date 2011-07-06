# -*- coding: utf-8 -*-
#
import types
import unittest
from couchdb.server import state

class StateTestCase(unittest.TestCase):

    def setUp(self):
        state.init()

    def test_add_fun(self):
        """should cache compiled function and its source code"""
        self.assertEqual(state.functions, [])
        self.assertEqual(state.functions_src, [])
        state.add_fun('def foo(bar): return baz')
        self.assertTrue(isinstance(state.functions[0], types.FunctionType))
        self.assertEqual(state.functions_src[0], 'def foo(bar): return baz')

    def test_add_fun_with_lib_context(self):
        """should compile function within context of view lib if it setted"""
        state.version = (1, 1, 0)
        state.add_lib({'foo': 'exports["bar"] = 42'})
        state.add_fun('def test(doc): return require("views/lib/foo")["bar"]')
        func = state.functions[0]
        self.assertEqual(func({}), 42)
        state.version = None

    def test_add_lib(self):
        """should cache view lib to module attribute"""
        self.assertEqual(state.view_lib, None)
        self.assertTrue(state.add_lib({'foo': 'bar'}))
        self.assertEqual(state.view_lib, {'foo': 'bar'})

    def test_reset(self):
        """should return True. always."""
        self.assertTrue(state.reset())

    def test_reset_clears_cache(self):
        """should clear function cache and query config"""
        state.functions.append('foo')
        state.functions_src.append('bar')
        state.query_config['baz'] = 42
        state.reset()
        self.assertEqual(state.functions, [])
        self.assertEqual(state.functions_src, [])
        self.assertEqual(state.query_config, {})

    def test_reset_and_update_query_config(self):
        """should reset query config and set new values to it"""
        state.query_config['foo'] = 'bar'
        state.reset({'foo': 'baz'})
        self.assertEqual(state.query_config, {'foo': 'baz'})


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(StateTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
