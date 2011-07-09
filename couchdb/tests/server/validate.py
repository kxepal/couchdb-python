# -*- coding: utf-8 -*-
#
import unittest
from textwrap import dedent
from inspect import getsource
from couchdb.server import compiler
from couchdb.server import exceptions
from couchdb.server import state
from couchdb.server import validate

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
        compiler.context['Forbidden'] = exceptions.Forbidden
        compiler.context['FatalError'] = exceptions.FatalError

    def tearDown(self):
        compiler.context.clear()

    def test_validate(self):
        """should return 1 (int) on successful validation"""
        self.assertEqual(
            validate.validate(self.funsrc, {'is_good': True}, {}, {}),
            1
        )

    def test_ddoc_validate(self):
        """should return 1 (int) on successful validation (0.11.0+ version)"""
        func = compiler.compile_func(self.funsrc, {})
        self.assertEqual(
            validate.ddoc_validate(func, {'is_good': True}, {}, {}),
            1
        )

    def test_validate_failure(self):
        """should except Forbidden exception for graceful deny"""
        func = compiler.compile_func(self.funsrc, {})
        self.assertRaises(
            exceptions.Forbidden,
            validate.ddoc_validate,
            func, {'is_good': False}, {}, {}
        )

    def test_assertions(self):
        """should count AssertionError as Forbidden"""
        func = compiler.compile_func(self.funsrc, {})
        self.assertRaises(
            exceptions.Forbidden,
            validate.ddoc_validate,
            func, {'is_good': False, 'try_assert': True}, {}, {}
        )

    def test_secobj(self):
        """should pass secobj argument to validate function (0.11.1+)"""
        funsrc = (
            'def validatefun(newdoc, olddoc, userctx, secobj):\n'
            '    assert isinstance(secobj, dict)\n'
        )
        func = compiler.compile_func(funsrc, {})
        state.version = (0, 11, 1)
        self.assertEqual(validate.ddoc_validate(func, {}, {}, {}, {}), 1)
        state.version = None

    def test_secobj_optional(self):
        """secobj argument could be optional"""
        state.version = (0, 11, 1)
        func = compiler.compile_func(self.funsrc, {})
        self.assertEqual(
            validate.ddoc_validate(func, {'is_good': True}, {}, {}, {}),
            1
        )
        state.version = None

    def test_viewserver_exception(self):
        """should rethow ViewServerException as is"""
        funsrc = (
            'def validatefun(newdoc, olddoc, userctx):\n'
            '    raise FatalError("validation", "failed")\n'
        )
        func = compiler.compile_func(funsrc, {})
        try:
            validate.ddoc_validate(func, {}, {}, {})
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
            validate.ddoc_validate(func, {}, {}, {})
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.Error))
            self.assertEqual(err.args[0], 'NameError')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ValidateTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
