# -*- coding: utf-8 -*-
#
import unittest
from textwrap import dedent
from inspect import getsource
from couchdb.server import compiler
from couchdb.server import exceptions
from couchdb.server import validate
from couchdb.server.mock import MockQueryServer

class ValidateTestCase(unittest.TestCase):

    def setUp(self):
        def validatefun(newdoc, olddoc, userctx):
            if newdoc.get('try_assert'):
                assert newdoc['is_good']
            if newdoc.get('is_good'):
                return True
            else:
                raise Forbidden('bad doc')

        self.funsrc = dedent(getsource(validatefun))
        self.server = MockQueryServer()

    def test_validate(self):
        """should return 1 (int) on successful validation"""
        result = validate.validate(
            self.server, self.funsrc, {'is_good': True}, {}, {})
        self.assertEqual(result, 1)

    def test_ddoc_validate(self):
        """should return 1 (int) on successful validation (0.11.0+ version)"""
        func = compiler.compile_func(self.funsrc, {})
        result = validate.ddoc_validate(
            self.server, func, {'is_good': True}, {}, {})
        self.assertEqual(result, 1)

    def test_validate_failure(self):
        """should except Forbidden exception for graceful deny"""
        func = compiler.compile_func(self.funsrc, {})
        self.assertRaises(
            exceptions.Forbidden,
            validate.ddoc_validate,
            self.server, func, {'is_good': False}, {}, {}
        )

    def test_assertions(self):
        """should count AssertionError as Forbidden"""
        func = compiler.compile_func(self.funsrc, {})
        self.assertRaises(
            exceptions.Forbidden,
            validate.ddoc_validate,
            self.server, func, {'is_good': False, 'try_assert': True}, {}, {}
        )

    def test_secobj(self):
        """should pass secobj argument to validate function (0.11.1+)"""
        funsrc = (
            'def validatefun(newdoc, olddoc, userctx, secobj):\n'
            '    assert isinstance(secobj, dict)\n'
        )
        func = compiler.compile_func(funsrc, {})
        server = MockQueryServer((0, 11, 1))
        result = validate.ddoc_validate(server, func, {}, {}, {}, {})
        self.assertEqual(result, 1)

    def test_secobj_optional(self):
        """secobj argument could be optional"""
        server = MockQueryServer((0, 11, 1))
        func = compiler.compile_func(self.funsrc, {})
        result = validate.ddoc_validate(
            server, func, {'is_good': True}, {}, {}, {})
        self.assertEqual(result, 1)

    def test_viewserver_exception(self):
        """should rethow ViewServerException as is"""
        funsrc = (
            'def validatefun(newdoc, olddoc, userctx):\n'
            '    raise FatalError("validation", "failed")\n'
        )
        func = compiler.compile_func(funsrc, {})
        try:
            validate.ddoc_validate(self.server, func, {}, {}, {})
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'validation')
            self.assertEqual(err.args[1], 'failed')

    def test_python_exception(self):
        """should raise Error exception instead of Python one to keep QS alive"""
        funsrc = (
            'def validatefun(newdoc, olddoc, userctx):\n'
            '    return foo\n'
        )
        func = compiler.compile_func(funsrc, {})
        try:
            validate.ddoc_validate(self.server, func, {}, {}, {})
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.Error))
            self.assertEqual(err.args[0], 'NameError')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ValidateTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
