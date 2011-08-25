# -*- coding: utf-8 -*-
#
import unittest

from couchdb.tests.server import compiler, ddoc, filters, mime, qs, render, \
                                 state, stream, validate, views


def suite():
    suite = unittest.TestSuite()
    suite.addTest(compiler.suite())
    suite.addTest(ddoc.suite())
    suite.addTest(filters.suite())
    suite.addTest(mime.suite())
    suite.addTest(qs.suite())
    suite.addTest(render.suite())
    suite.addTest(state.suite())
    suite.addTest(stream.suite())
    suite.addTest(validate.suite())
    suite.addTest(views.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
