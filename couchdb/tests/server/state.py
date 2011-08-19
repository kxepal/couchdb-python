# -*- coding: utf-8 -*-
#
import types
import unittest
from couchdb.server import state
from couchdb.server.mock import MockQueryServer

class StateTestCase(unittest.TestCase):

    def setUp(self):
        self.server = MockQueryServer()

    def test_add_fun(self):
        """should cache compiled function and its source code"""
        server = self.server
        self.assertEqual(server.state['functions'], [])
        self.assertEqual(server.state['functions_src'], [])
        state.add_fun(server, 'def foo(bar): return baz')
        func = server.state['functions'][0]
        funstr = server.state['functions_src'][0]
        self.assertTrue(isinstance(func, types.FunctionType))
        self.assertEqual(funstr, 'def foo(bar): return baz')

    def test_add_fun_with_lib_context(self):
        """should compile function within context of view lib if it setted"""
        funsrc = (
            'def test(doc):\n'
            '  return require("views/lib/foo")["bar"]')
        server = MockQueryServer(version=(1, 1, 0))
        state.add_lib(server, {'foo': 'exports["bar"] = 42'})
        state.add_fun(server, funsrc)
        func = server.state['functions'][0]
        self.assertEqual(func({}), 42)

    def test_add_lib(self):
        """should cache view lib to module attribute"""
        server = self.server
        self.assertEqual(server.state['view_lib'], None)
        self.assertTrue(state.add_lib(server, {'foo': 'bar'}))
        self.assertEqual(server.state['view_lib'], {'foo': 'bar'})

    def test_reset(self):
        """should return True. always."""
        server = self.server
        self.assertTrue(state.reset(server))

    def test_reset_clears_cache(self):
        """should clear function cache and query config"""
        server = self.server
        server.state['functions'].append('foo')
        server.state['functions_src'].append('bar')
        server.state['query_config']['baz'] = 42
        state.reset(server)
        self.assertEqual(server.state['functions'], [])
        self.assertEqual(server.state['functions_src'], [])
        self.assertEqual(server.state['query_config'], {})

    def test_reset_and_update_query_config(self):
        """should reset query config and set new values to it"""
        server = self.server
        server.state['query_config']['foo'] = 'bar'
        state.reset(server, {'foo': 'baz'})
        self.assertEqual(server.state['query_config'], {'foo': 'baz'})


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(StateTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
