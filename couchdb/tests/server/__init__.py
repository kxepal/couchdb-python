# -*- coding: utf-8 -*-
#
import unittest

from couchdb.tests.server import compiler


def suite():
    suite = unittest.TestSuite()
    suite.addTest(compiler.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
