# -*- coding: utf-8 -*-
#
import unittest
from types import FunctionType
from couchdb.server import ddoc
from couchdb.server import exceptions

class DDocTestCase(unittest.TestCase):

    def setUp(self):
        def proxy(func, *args):
            return func(*args)
        ddoc.commands['bar'] = proxy

    def tearDown(self):
        ddoc.ddocs.clear()
        ddoc.commands.clear()

    def test_register_ddoc(self):
        """should register design documents"""
        self.assertTrue(ddoc.ddoc('new', 'foo', {'bar': 'baz'}))
        self.assertTrue(ddoc.ddoc('new', 'bar', {'baz': 'foo'}))
        self.assertTrue(ddoc.ddoc('new', 'baz', {'foo': 'bar'}))
        self.assertEqual(
            ddoc.ddocs,
            {'foo': {'bar': 'baz'},
             'bar': {'baz': 'foo'},
             'baz': {'foo': 'bar'}}
        )

    def test_call_ddoc_func(self):
        """should call design function by specified path"""
        ddoc.ddoc('new', 'foo', {'bar': 'def boo(): return True'})
        self.assertTrue(ddoc.ddoc('foo', ['bar'], []))

    def test_call_cached_ddoc_func(self):
        ddoc.ddoc('new', 'foo', {'bar': 'def boo(): return True'})
        self.assertTrue(ddoc.ddoc('foo', ['bar'], []))
        self.assertTrue(isinstance(ddoc.ddocs['foo']['bar'], FunctionType))
        self.assertTrue(ddoc.ddoc('foo', ['bar'], []))

    def test_fail_for_unknown_ddoc_command(self):
        """should raise FatalError on unknown ddoc command"""
        ddoc.ddoc('new', 'foo', {'bar': 'def boo(): return True'})
        try:
            ddoc.ddoc('foo', ['boo', 'bar'], [])
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'unknown_command')

    def test_fail_process_unregistered_ddoc(self):
        """should raise FatalError if ddoc was not registered
        before design function call"""
        self.assertRaises(
            exceptions.FatalError,
            ddoc.ddoc, 'foo', ['bar', 'baz'], []
        )

    def test_fail_call_unknown_func(self):
        """should raise Error for unknown design function call"""
        ddoc.ddoc('new', 'foo', {'bar': {'baz': 'def boo(): return True'}})
        try:
            ddoc.ddoc('foo', ['bar', 'zap'], [])
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.Error))
            self.assertEqual(err.args[0], 'not_found')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DDocTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
