# -*- coding: utf-8 -*-
#
import unittest

from couchdb.tests.server import compiler, state


def suite():
    suite = unittest.TestSuite()
    suite.addTest(compiler.suite())
    suite.addTest(state.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
