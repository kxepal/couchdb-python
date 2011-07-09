# -*- coding: utf-8 -*-
#
import unittest
from textwrap import dedent
from inspect import getsource
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
                raise exceptions.Forbidden('bad doc')

        self.func = validatefun
        self.funsrc = dedent(getsource(validatefun))

    def test_validate(self):
        """should return 1 (int) on successful validation"""
        self.assertEqual(
            validate.validate(self.funsrc, {'is_good': True}, {}, {}),
            1
        )

    def test_ddoc_validate(self):
        """should return 1 (int) on successful validation (0.11.0+ version)"""
        self.assertEqual(
            validate.ddoc_validate(self.func, {'is_good': True}, {}, {}),
            1
        )

    def test_validate_failure(self):
        """should except Forbidden exception for graceful deny"""
        self.assertRaises(
            exceptions.Forbidden,
            validate.ddoc_validate,
            self.func, {'is_good': False}, {}, {}
        )

    def test_assertions(self):
        """should count AssertionError as Forbidden"""
        self.assertRaises(
            exceptions.Forbidden,
            validate.ddoc_validate,
            self.func, {'is_good': False, 'try_assert': True}, {}, {}
        )

    def test_secobj(self):
        """should pass secobj argument to validate function (0.11.1+)"""
        def validatefun(newdoc, olddoc, userctx, secobj):
            assert isinstance(secobj, dict)
        state.version = (0, 11, 1)
        self.assertEqual(validate.ddoc_validate(validatefun, {}, {}, {}, {}), 1)
        state.version = None

    def test_secobj_optional(self):
        """secobj argument could be optional"""
        state.version = (0, 11, 1)
        self.assertEqual(
            validate.ddoc_validate(self.func, {'is_good': True}, {}, {}, {}),
            1
        )
        state.version = None


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ValidateTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
