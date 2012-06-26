# -*- coding: utf-8 -*-
#
import unittest
from types import FunctionType
from couchdb.server import ddoc
from couchdb.server import exceptions
from couchdb.server.mock import MockQueryServer

class DDocTestCase(unittest.TestCase):

    def setUp(self):
        def proxy(server, func, *args):
            return func(*args)
        self.ddoc = ddoc.DDoc(bar=proxy)
        self.server = MockQueryServer()

    def test_register_ddoc(self):
        """should register design documents"""
        self.assertTrue(self.ddoc(self.server, 'new', 'foo', {'bar': 'baz'}))
        self.assertTrue(self.ddoc(self.server, 'new', 'bar', {'baz': 'foo'}))
        self.assertTrue(self.ddoc(self.server, 'new', 'baz', {'foo': 'bar'}))
        self.assertEqual(
            self.ddoc.cache,
            {'foo': {'bar': 'baz'},
             'bar': {'baz': 'foo'},
             'baz': {'foo': 'bar'}}
        )

    def test_call_ddoc_func(self):
        """should call design function by specified path"""
        self.ddoc(self.server, 'new', 'foo', {'bar': 'def boo(): return True'})
        self.assertTrue(self.ddoc(self.server, 'foo', ['bar'], []))

    def test_call_cached_ddoc_func(self):
        self.ddoc(self.server, 'new', 'foo', {'bar': 'def boo(): return True'})
        self.assertTrue(self.ddoc(self.server, 'foo', ['bar'], []))
        self.assertTrue(isinstance(self.ddoc.cache['foo']['bar'], FunctionType))
        self.assertTrue(self.ddoc(self.server, 'foo', ['bar'], []))

    def test_fail_for_unknown_ddoc_command(self):
        """should raise FatalError on unknown ddoc command"""
        self.ddoc(self.server, 'new', 'foo', {'bar': 'def boo(): return True'})
        try:
            self.ddoc(self.server, 'foo', ['boo', 'bar'], [])
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'unknown_command')

    def test_fail_process_unregistered_ddoc(self):
        """should raise FatalError if ddoc was not registered
        before design function call"""
        self.assertRaises(
            exceptions.FatalError,
            self.ddoc, self.server, 'foo', ['bar', 'baz'], []
        )

    def test_fail_call_unknown_func(self):
        """should raise Error for unknown design function call"""
        self.ddoc(self.server, 'new', 'foo', {'bar': {'baz': 'pass'}})
        try:
            self.ddoc(self.server, 'foo', ['bar', 'zap'], [])
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.Error))
            self.assertEqual(err.args[0], 'not_found')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DDocTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
