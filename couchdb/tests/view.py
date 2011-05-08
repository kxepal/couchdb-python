# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2008 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
import doctest
import inspect
import sys
import textwrap
import unittest
from StringIO import StringIO
from os.path import normpath, join, dirname

from couchdb import view
from testutil import QueryServer, TestRunner

VIEW_SERVER = normpath(join(dirname(__file__), '../view.py'))

TRUNK = (999, 999, 999) # assume latest one
COUCHDB_VERSION = TRUNK

def make_ddoc(fun_path, fun_str):
    doc = {'_id': 'foo'}
    d = doc
    while fun_path:
        l = p = fun_path.pop(0)
        if fun_path:
            d[p] = {}
            d = d[p]
    d[l] = fun_str
    return doc


class QueryServerMixIn(unittest.TestCase):
    def setUp(self):
        self.qs = QueryServer(VIEW_SERVER, version=COUCHDB_VERSION)

    def tearDown(self):
        self.qs.close()

class TestFuncsMixIn(object):

    @property
    def funs(self, cache={}):
        def funs_to_string(d):
            for k, f in d.items():
                if k != 'self':
                    d[k] = textwrap.dedent(inspect.getsource(f))
            return d
        if cache:
            return cache
        ns = getattr(self, 'functions', lambda: {})()
        cache = dict([(k, textwrap.dedent(inspect.getsource(v)))
                      for k, v in ns.items() if k != 'self'])
        return cache

class ViewTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def emit_none(doc):
            pass

        def not_emit_but_return(doc):
            return [['foo', doc['a']]]

        def emit_twise(doc):
            yield 'foo', doc['a']
            yield 'bar', doc['a']

        def emit_changes(doc):
            assert doc['_id'] == 'foo'
            doc['_id'] = 'bar'
            yield doc['_id'], None

        def emit_once(doc):
            yield 'baz', doc['a']

        def emit_with_lib(doc):
            yield 'foo', require('views/lib/foo')['bar']

        def reduce_values_length(keys, values, rereduce):
            return len(values)

        def reduce_values_sum(keys, values, rereduce):
            return sum(values)

        def show_simple(doc, req):
            log('ok')
            return ' - '.join([doc['title'], doc['body']])

        return locals()

    def test_reset(self):
        ''' should reset '''
        resp = self.qs.run(['reset'])
        self.assertEquals(resp, True)

    def test_reset_should_not_erase_ddocs(self):
        ''' should not erase ddocs on reset '''
        def test_before_0_11_0_version():
            fun = self.funs['show_simple']
            ddoc = make_ddoc(['shows', 'simple'], fun)
            resp = self.qs.teach_ddoc(ddoc)
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command ddoc'})
            self.assertEqual(self.qs.close(), 1)

        def test_for_0_11_0_version_and_later():
            fun = self.funs['show_simple']
            ddoc = make_ddoc(['shows', 'simple'], fun)
            resp = self.qs.teach_ddoc(ddoc)
            self.assertEqual(self.qs.run(['reset']), True)
            self.qs.send_ddoc(ddoc, ['shows', 'simple'],
                              [{'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['resp', {'body': 'best ever - doc body'}])

        if COUCHDB_VERSION < (0, 11, 0):
            test_before_0_11_0_version()
        else:
            test_for_0_11_0_version_and_later()

    def test_run_map_funs(self):
        ''' should run map funs '''
        self.qs.reset()
        self.assertEqual(self.qs.run(['add_fun', self.funs['emit_twise']]), True)
        self.assertEqual(self.qs.run(['add_fun', self.funs['emit_once']]), True)
        rows = self.qs.run(['map_doc', {'_id': 'test_doc', 'a': 'b'}])
        self.assertEqual(rows[0][0], ['foo', 'b'])
        self.assertEqual(rows[0][1], ['bar', 'b'])
        self.assertEqual(rows[1][0], ['baz', 'b'])

    def test_run_map_fun_that_do_nothing(self):
        ''' should not fail if map func do nothing '''
        self.qs.reset()
        self.assertEqual(self.qs.run(['add_fun', self.funs['emit_none']]), True)
        rows = self.qs.run(['map_doc', {'_id': 'test_doc', 'a': 'b'}])
        self.assertEqual(rows, [[]])

    def test_run_map_funs_that_is_not_generator(self):
        ''' should allow return value instead of generate it '''
        self.qs.reset()
        self.assertEqual(self.qs.run(['add_fun', self.funs['not_emit_but_return']]), True)
        rows = self.qs.run(['map_doc', {'_id': 'test_doc', 'a': 'b'}])
        self.assertEqual(rows[0][0], ['foo', 'b'])

    def test_documents_seal(self):
        ''' should not allow cascade document changes within map funs '''
        self.qs.reset()
        self.assertEqual(self.qs.run(['add_fun', self.funs['emit_changes']]), True)
        self.assertEqual(self.qs.run(['add_fun', self.funs['emit_changes']]), True)
        rows = self.qs.run(['map_doc', {'_id': 'foo'}])
        self.assertEqual(rows[0][0], ['bar', None])
        self.assertEqual(rows[0][0], ['bar', None])

    def test_add_lib(self):
        ''' should add and require lib '''
        def test_before_0_11_0_version():
            resp = self.qs.run(['add_lib', {'foo': 'exports["bar"] = "bar"'}])
            valid_resp = {'error': 'unknown_command',
                          'reason': 'unknown command add_lib'}
            self.assertEqual(resp, valid_resp)
            self.assertEqual(self.qs.close(), 1)

        def test_from_0_11_0_till_1_1_0_versions():
            resp = self.qs.run(['add_lib', {'foo': 'exports["bar"] = "bar"'}])
            valid_resp = ['error', 'unknown_command', 'unknown command add_lib']
            self.assertEqual(resp, valid_resp)
            self.assertEqual(self.qs.close(), 1)

        def test_for_1_1_0_version_and_later():
            self.qs.run(['add_lib', {'foo': 'exports["bar"] = "bar"'}])
            self.qs.run(['add_fun', self.funs['emit_with_lib']])
            rows = self.qs.run(['map_doc', {'_id': 'foo', 'bar': 'baz'}])
            self.assertEqual(rows[0][0], ['foo', 'bar'])

        if COUCHDB_VERSION < (0, 11, 0):
            test_before_0_11_0_version()
        elif COUCHDB_VERSION < (1, 1, 0):
            test_from_0_11_0_till_1_1_0_versions()
        else:
            test_for_1_1_0_version_and_later()

    def test_reduce(self):
        ''' should reduce '''
        fun = self.funs['reduce_values_length']
        self.qs.reset()
        kvs = [(i, i * 2) for i in xrange(10)]
        self.assertEqual(self.qs.run(['reduce', [fun], kvs]), [True, [10]])

    def test_rereduce(self):
        ''' should rereduce '''
        fun = self.funs['reduce_values_sum']
        self.qs.reset()
        resp = self.qs.run(['rereduce', [fun], range(10)])
        self.assertEqual(resp, [True, [45]])

    def test_reduce_with_no_records(self):
        ''' should not fail if map func yields no record. see bug #163 '''
        fun = self.funs['reduce_values_sum']
        self.qs.reset()
        self.assertEquals(self.qs.run(['reduce', [fun], []]), [True, [0]])

    def test_learn_design_docs(self):
        ''' should learn design docs '''
        def test_before_0_11_0_version():
            ddoc = {'_id': 'foo'}
            self.qs.reset()
            resp = self.qs.teach_ddoc(ddoc)
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command ddoc'})
            self.assertEqual(self.qs.close(), 1)
        def test_for_0_11_0_version_and_later():
            ddoc = {'_id': 'foo'}
            self.qs.reset()
            resp = self.qs.teach_ddoc(ddoc)
            self.assertEqual(resp, True)

        if COUCHDB_VERSION < (0, 11, 0):
            test_before_0_11_0_version()
        else:
            test_for_0_11_0_version_and_later()

    def test_allows_imports_at_function_top_level(self):
        fun = '''
        import datetime
        def mapfun(doc, datetime=datetime):
          date = datetime.datetime(2010, 02, 14, 02, 31, 30)
          yield doc['_id'], date.isoformat()
        '''
        self.assertEqual(self.qs.run(['add_fun', textwrap.dedent(fun)]), True)
        rows = self.qs.run(['map_doc', {'_id': 'foo'}])
        self.assertEqual(rows[0][0], ['foo', '2010-02-14T02:31:30'])

    def test_multiple_functions_definition_cause_error(self):
        def test_before_0_11_0_version():
            fun = '''
            def helper():
              return "bar"

            def mapfun(doc, helper=helper):
              yield doc['_id'], helper()

            '''
            resp = self.qs.run(['add_fun', textwrap.dedent(fun)])
            self.assertEqual(resp['error'], 'compilation_error')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            fun = '''
            def helper():
              return "bar"

            def mapfun(doc, helper=helper):
              yield doc['_id'], helper()

            '''
            resp = self.qs.run(['add_fun', textwrap.dedent(fun)])
            self.assertEqual(resp[0], 'error')
            self.assertEqual(resp[1], 'compilation_error')
            self.assertEqual(self.qs.close(), 0)

        if COUCHDB_VERSION < (0, 11, 0):
            test_before_0_11_0_version()
        else:
            test_for_0_11_0_version_and_later()

    def test_custom_variable_definition_at_top_level_cause_error(self):
        def test_before_0_11_0_version():
            fun = '''
            cache = {}
            def mapfun(doc):
              if not doc['_id'] in cache:
                yield doc['_id'], doc
            '''
            resp = self.qs.run(['add_fun', textwrap.dedent(fun)])
            self.assertEqual(resp['error'], 'compilation_error')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            fun = '''
            cache = {}
            def mapfun(doc):
              if not doc['_id'] in cache:
                yield doc['_id'], doc
            '''
            resp = self.qs.run(['add_fun', textwrap.dedent(fun)])
            self.assertEqual(resp[0], 'error')
            self.assertEqual(resp[1], 'compilation_error')
            self.assertEqual(self.qs.close(), 0)

        if COUCHDB_VERSION < (0, 11, 0):
            test_before_0_11_0_version()
        else:
            test_for_0_11_0_version_and_later()

    def test_map_exceptions_should_not_crush_viewserver(self):
        def test_before_0_11_0_version():
            self.qs.reset()
            self.qs.run(['add_fun', self.funs['emit_once']])
            resp = self.qs.run(['map_doc', {'_id': 'foo'}])
            self.assertEqual(resp['error'], 'KeyError')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            self.qs.reset()
            self.qs.run(['add_fun', self.funs['emit_once']])
            resp = self.qs.run(['map_doc', {'_id': 'foo'}])
            self.assertEqual(resp[0], 'error')
            self.assertEqual(resp[1], 'KeyError')
            self.assertEqual(self.qs.close(), 0)

        if COUCHDB_VERSION < (0, 11, 0):
            test_before_0_11_0_version()
        else:
            test_for_0_11_0_version_and_later()

class ValidateTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def validate_forbidden(newdoc, olddoc, userctx):
            if newdoc.get('bad', False):
                raise Forbidden('bad doc')

        def validate_with_secobj(newdoc, olddoc, userctx, secobj):
            if newdoc.get('bad', False):
                raise Forbidden('bad doc')

        def validate_via_assert(newdoc, olddoc, userctx):
            assert olddoc['author'] == newdoc['author'], \
                   'changing author is not allowed'

        return locals()

    def setUp(self):
        super(ValidateTestCase, self).setUp()
        def setUp_before_0_11_0_version():
            self.qs.reset()

        def setUp_for_0_11_0_version_and_later():
            fun = self.funs['validate_forbidden']
            self.ddoc = make_ddoc(['validate_doc_update'], fun)
            self.qs.teach_ddoc(self.ddoc)

        if COUCHDB_VERSION < (0, 11, 0):
            setUp_before_0_11_0_version()
        else:
            setUp_for_0_11_0_version_and_later()

    def test_validate_all_good_updates(self):
        ''' should allow good updates '''
        def test_versions_before_0_9_0():
            fun = self.funs['validate_forbidden']
            self.qs.send(['validate', fun, {'good': True}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command validate'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_11_0():
            fun = self.funs['validate_forbidden']
            self.qs.send(['validate', fun, {'good': True}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, 1)

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['validate_doc_update'],
                                         [{'good': True}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, 1)

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_9_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_validate_reject_invalid_updates(self):
        ''' should reject invalid updates '''
        def test_versions_before_0_9_0():
            fun = self.funs['validate_forbidden']
            self.qs.send(['validate', fun, {'good': True}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command validate'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_11_0():
            fun = self.funs['validate_forbidden']
            self.qs.send(['validate', fun, {'bad': True}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'forbidden': 'bad doc'})

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['validate_doc_update'],
                                         [{'bad': True}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'forbidden': 'bad doc'})

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_9_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_validate_with_security_object(self):
        ''' should accept secobj argument since 0.11.1 '''
        def test_versions_before_0_9_0():
            fun = self.funs['validate_forbidden']
            self.qs.send(['validate', fun, {'good': True}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command validate'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_11_0():
            fun = self.funs['validate_forbidden']
            self.qs.send(['validate', fun, {'good': True}, {}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(self.qs.close(), 1)

        def test_for_0_11_0_version():
            self.qs.send_ddoc(self.ddoc, ['validate_doc_update'],
                                         [{'good': True}, {}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(self.qs.close(), 1)

        def test_for_0_11_1_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['validate_doc_update'],
                                         [{'good': True}, {}, {}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, 1)

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_9_0_till_0_11_0()
        elif COUCHDB_VERSION == (0, 11, 0):
            test_for_0_11_0_version()
        else:
            test_for_0_11_1_version_and_later()


    def test_validate_via_assert(self):
        ''' should track assertion error as forbidden '''
        def test_versions_before_0_9_0():
            fun = self.funs['validate_via_assert']
            self.qs.send(['validate', fun,
                          {'author': 'Mike'}, {'author': 'John'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command validate'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_11_0():
            fun = self.funs['validate_via_assert']
            self.qs.send(['validate', fun,
                          {'author': 'Mike'}, {'author': 'John'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'forbidden': 'changing author is not allowed'})

        def test_for_0_11_0_version_and_later():
            fun = self.funs['validate_via_assert']
            self.ddoc = make_ddoc(['validate_doc_update'], fun)
            self.qs.teach_ddoc(self.ddoc)
            self.qs.send_ddoc(self.ddoc, ['validate_doc_update'],
                                         [{'author': 'Mike'}, {'author': 'John'},
                                          {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'forbidden': 'changing author is not allowed'})

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_9_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()


class ShowTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def show_simple(doc, req):
            log('ok')
            return ' - '.join([doc['title'], doc['body']])

        def show_headers(doc, req):
            resp = {'code': 200, 'headers': {'X-Couchdb-Python': 'Hello, world!'}}
            resp['body'] = ' - '.join([doc['title'], doc['body']])
            return resp

        def show_with_require(doc, req):
            stuff = require('lib/utils.py')
            return ' - '.join([stuff['title'], stuff['body']])

        def show_with_lazy_require(doc, req):
            stuff = require('lib/utils.py')
            return ' - '.join([stuff['title'](), stuff['body']()])

        def show_provides_old(doc, req):
            def html():
                return '<html><body>%s</body></html>' % doc['_id']
            def xml():
                return '<root><doc id="%s" /></root>' % doc['_id']
            def foo():
                return 'foo? bar! bar!'
            register_type('foo', 'application/foo', 'application/x-foo')
            return response_with(req, {
                'html': html,
                'xml': xml,
                'foo': foo,
                'fallback': 'html'
            })

        def show_provides(doc, req):
            def html():
                return '<html><body>%s</body></html>' % doc['_id']
            def xml():
                return '<root><doc id="%s" /></root>' % doc['_id']
            def foo():
                return 'foo? bar! bar!'
            register_type('foo', 'application/foo', 'application/x-foo')
            provides('html', html)
            provides('xml', xml)
            provides('foo', foo)

        return locals()

    def test_show(self):
        ''' should show '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_simple']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_simple']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'body': 'best ever - doc body'})

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_simple']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['resp', {'body': 'best ever - doc body'}])

        def test_for_0_11_0_version_and_later():
            fun = self.funs['show_simple']
            ddoc = make_ddoc(['shows', 'simple'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'simple'],
                              [{'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['resp', {'body': 'best ever - doc body'}])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_with_require(self):
        ''' should show with data import '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            ddoc = {
                '_id': 'foo',
                'shows':{
                    'with_require': self.funs['show_with_require'],
                },
                'lib': {
                    'utils.py': (
                        "if True:\n"
                        "  exports['title'] = 'best ever' \n"
                        "  exports['body'] = 'doc body'"
                    )
                }
            }
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'with_require'],
                              [{'title': 'some title', 'body': 'some body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['resp', {'body': 'best ever - doc body'}])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_with_nested_require(self):
        ''' should show with relative data import '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            ddoc = {
                '_id': 'foo',
                'shows': {
                    'with_require': self.funs['show_with_require'],
                },
                'lib': {
                    'helper': {
                        'a': {
                            'b': {
                                'c': "exports.update(require('lib/helper/a/d/e'))",
                            },
                            'd': {
                                'e': "exports.update(require('../.././././f'))"
                            }
                        },
                        'f': "exports.update(require('.././helper/./g'))",
                        'g': ("exports['title'] = 'best ever'\n"
                              "exports['body'] = 'doc body'")
                    },
                    'utils.py': (
                        "def help():\n"
                        "  return require('./helper/a/b/c') \n"
                        "stuff = help()\n"
                        "exports['title'] = stuff['title'] \n"
                        "exports['body'] = stuff['body']")
                }
            }
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'with_require'],
                              [{'title': 'some title', 'body': 'some body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['resp', {'body': 'best ever - doc body'}])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_with_explicit_circular_require_calls(self):
        ''' should not fall into infinity loop for circular require calls '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            ddoc = {
                '_id': 'foo',
                'shows':{
                    'with_require': self.funs['show_with_require'],
                },
                'lib': {
                    'stuff': (
                        "exports['utils'] = require('./utils.py') \n"
                        "exports['body'] = 'doc forever!'"),
                    'helper': (
                        "exports['title'] = 'best ever' \n"
                        "exports['body'] = require('./stuff')"),
                    'utils.py': (
                        "def help():\n"
                        "  return require('./helper') \n"
                        "stuff = help()\n"
                        "exports['title'] = stuff['title'] \n"
                        "exports['body'] = stuff['body']")
                }
            }
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'with_require'],
                              [{'title': 'some title', 'body': 'some body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp[0], 'error')
            self.assertEqual(resp[1], 'compilation_error')

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_with_implicit_circular_require_calls(self):
        ''' should not fall into infinity loop for circular require calls '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_with_lazy_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_with_lazy_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_with_lazy_require']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            ddoc = {
                '_id': 'foo',
                'shows':{
                    'show_with_lazy_require': self.funs['show_with_lazy_require'],
                },
                'lib': {
                    'body': (
                        "def body():\n"
                        "  require('./title')['title']() \n"
                        "  return 'doc forever!' \n"
                        "exports['body'] = body"),
                    'title': (
                        "def title():\n"
                        "  require('./body')['body']() \n"
                        "  return 'best ever' \n"
                        "exports['title'] = title"),
                    'utils.py': (
                        "def title():\n"
                        "  return require('./title')['title'] \n"
                        "def body():\n"
                        "  return require('./body')['body'] \n"
                        "exports['title'] = title() \n"
                        "exports['body'] = body()")
                }
            }
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'show_with_lazy_require'],
                              [{'title': 'some title', 'body': 'some body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp[0], 'error')
            self.assertEqual(resp[1], 'render_error', resp[2])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_with_eggs_as_modules(self):
        ''' should import top level namespace of stored base64 encoded egg '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_with_require']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'render_error')
            self.assertEqual(self.qs.close(), 0)

        def test_for_0_11_0_version_and_later():
            # TODO: testutils refactoring needs
            self.qs.close()
            self.qs.__init__(VIEW_SERVER, COUCHDB_VERSION, '--enable-eggs')
            ddoc = {
                '_id': 'foo',
                'shows':{
                    'show_with_require': self.funs['show_with_require'],
                },
                'lib': {
                    'egg': (''
'UEsDBBQAAAAIAKx1qD6TBtcyAwAAAAEAAAAdAAAARUdHLUlORk8vZGVwZW5kZW5jeV9saW5rcy50'
'eHTjAgBQSwMEFAAAAAgArHWoPrbiy8BxAAAAuQAAABEAAABFR0ctSU5GTy9QS0ctSU5GT/NNLUlM'
'SSxJ1A1LLSrOzM+zUjDUM+Dl8kvMTbVSSE1P5+VClQguzc1NLKq0Ugj18/bzD/fj5fLIz03VLUhM'
'T0UScywtycgvwhDQTc1NzMxBEvbJTE7NK0bW6ZJanFyUWVACthEuGpCTWJKWX5SLJAQAUEsDBBQA'
'AAAIAKx1qD7n9n20agAAAKYAAAAUAAAARUdHLUlORk8vU09VUkNFUy50eHQrTi0pLdArqOTlSk1P'
'1wNi3cy8tHz9AG93XU8/N3804WD/0CBn12C9kooSNJmU1ILUvJTUvOTK+JzMvOxiLEpK8gvic1LL'
'UnMgcqV5mWWpRcWp+vHxmXmZJfHxYFfARQtLU4tLMvPzwKIAUEsDBBQAAAAIAKx1qD6I9wVkCwAA'
'AAkAAAAWAAAARUdHLUlORk8vdG9wX2xldmVsLnR4dCvNyyxLLSpO5QIAUEsDBBQAAAAIAKx1qD6T'
'BtcyAwAAAAEAAAARAAAARUdHLUlORk8vemlwLXNhZmXjAgBQSwMEFAAAAAgAunSoPiupvPYyAAAA'
'OgAAABQAAAB1bml2ZXJzZS9xdWVzdGlvbi5weeOKj0/MyYmPV7BViFZPTy2JT8wrLk8tUo/l4kpJ'
'TVNAiGhoWnEpAEFRaklpUZ6CiREAUEsDBBQAAAAIAKx1qD71mOyCrAAAAAYBAAAVAAAAdW5pdmVy'
'c2UvcXVlc3Rpb24ucHljy/3Ey9VQdMw3mQEKGIHYAYiLxYBECgNDOiNDFJDByNDCwBDFyJDCxBCs'
'wQyUKuECEumpJfGJecXlqUUo+p1B+lkYwNqCNZiADL9MLSCpwYBJlIAkkkozc1JiklIyi0v0yjPz'
'jI1iUtPTY0rzMstSi4pTYwpLU4tLMvPz9Aoqg0B6QEYXM4Ft8wMbX8IOJOLjE3Ny4uPBKsCiYFYQ'
'E4p1QSD3lYAIeyaYKZxMAFBLAwQUAAAACADJdKg+38+n3x8AAAAdAAAAFAAAAHVuaXZlcnNlL19f'
'aW5pdF9fLnB5SyvKz1UozcssSy0qTlXIzC3ILypRKCxNLS7JzM8DAFBLAwQUAAAACACsdag+VUoJ'
'NoEAAAC4AAAAFQAAAHVuaXZlcnNlL19faW5pdF9fLnB5Y8v9xMs1q+iYbzIDFDABsQMQFwsCiRQG'
'hmwGhhxGhihGBsYURoZgDZC0BiNIngNIFJamFpdk5uf5gcVLQEKleZllqUXFqSXI8mAdQSBCgwFG'
'lGgBiaTSzJyUmKSUzOISvfLMPGOjmNT09BiYGTHx8Zl5mSXx8XoFlSUg3fZgm0G6AVBLAQIUABQA'
'AAAIAKx1qD6TBtcyAwAAAAEAAAAdAAAAAAAAAAAAAAC2gQAAAABFR0ctSU5GTy9kZXBlbmRlbmN5'
'X2xpbmtzLnR4dFBLAQIUABQAAAAIAKx1qD624svAcQAAALkAAAARAAAAAAAAAAAAAAC2gT4AAABF'
'R0ctSU5GTy9QS0ctSU5GT1BLAQIUABQAAAAIAKx1qD7n9n20agAAAKYAAAAUAAAAAAAAAAAAAAC2'
'gd4AAABFR0ctSU5GTy9TT1VSQ0VTLnR4dFBLAQIUABQAAAAIAKx1qD6I9wVkCwAAAAkAAAAWAAAA'
'AAAAAAAAAAC2gXoBAABFR0ctSU5GTy90b3BfbGV2ZWwudHh0UEsBAhQAFAAAAAgArHWoPpMG1zID'
'AAAAAQAAABEAAAAAAAAAAAAAALaBuQEAAEVHRy1JTkZPL3ppcC1zYWZlUEsBAhQAFAAAAAgAunSo'
'PiupvPYyAAAAOgAAABQAAAAAAAAAAAAAALaB6wEAAHVuaXZlcnNlL3F1ZXN0aW9uLnB5UEsBAhQA'
'FAAAAAgArHWoPvWY7IKsAAAABgEAABUAAAAAAAAAAAAAALaBTwIAAHVuaXZlcnNlL3F1ZXN0aW9u'
'LnB5Y1BLAQIUABQAAAAIAMl0qD7fz6ffHwAAAB0AAAAUAAAAAAAAAAAAAAC2gS4DAAB1bml2ZXJz'
'ZS9fX2luaXRfXy5weVBLAQIUABQAAAAIAKx1qD5VSgk2gQAAALgAAAAVAAAAAAAAAAAAAAC2gX8D'
'AAB1bml2ZXJzZS9fX2luaXRfXy5weWNQSwUGAAAAAAkACQBZAgAAMwQAAAAA'),
                    'utils.py': (
                        "universe = require('lib/egg')['universe'] \n"
                        "exports['title'] = 'The Answer' \n"
                        "exports['body'] = str(universe.question.get_answer())")
                }
            }
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'show_with_require'],
                              [{'title': 'some title', 'body': 'some body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['resp', {'body': 'The Answer - 42'}])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_with_headers(self):
        ''' should show headers '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_headers']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_headers']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}])
            resp = self.qs.recv()
            valid_resp = {
                'headers': {'X-Couchdb-Python': 'Hello, world!'},
                'code': 200,
                'body': 'best ever - doc body'
            }
            self.assertEqual(resp, valid_resp)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_headers']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            valid_resp = ['resp', {
                'headers': {'X-Couchdb-Python': 'Hello, world!'},
                'code': 200,
                'body': 'best ever - doc body'
            }]
            self.assertEqual(resp, valid_resp)

        def test_for_0_11_0_version_and_later():
            fun = self.funs['show_headers']
            ddoc = make_ddoc(['shows', 'headers'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'headers'],
                              [{'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            valid_resp = ['resp', {
                'headers': {'X-Couchdb-Python': 'Hello, world!'},
                'code': 200,
                'body': 'best ever - doc body'
            }]
            self.assertEqual(resp, valid_resp)

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_provides_match(self):
        ''' should match mime type '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_headers']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})

        def test_versions_since_0_9_0_till_0_10_0():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'text/html,application/atom+xml; q=0.9'}}
            fun = self.funs['show_provides_old']
            self.qs.send(['show_doc', fun, doc, req])
            resp = self.qs.recv()
            self.assertTrue('text/html' in resp['headers']['Content-Type'])
            self.assertEqual(resp['body'], '<html><body>couch</body></html>')

        def test_versions_since_0_10_0_till_0_11_0():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'text/html,application/atom+xml; q=0.9'}}
            fun = self.funs['show_provides']
            self.qs.send(['show', fun, doc, req])
            token, resp = self.qs.recv()
            self.assertEqual(token, 'resp')
            self.assertTrue('text/html' in resp['headers']['Content-Type'])
            self.assertEqual(resp['body'], '<html><body>couch</body></html>')

        def test_for_0_11_0_version_and_later():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'text/html,application/atom+xml; q=0.9'}}
            fun = self.funs['show_provides']
            ddoc = make_ddoc(['shows', 'provides'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'provides'], [doc, req])
            token, resp = self.qs.recv()
            self.assertEqual(token, 'resp')
            self.assertTrue('text/html' in resp['headers']['Content-Type'])
            self.assertEqual(resp['body'], '<html><body>couch</body></html>')

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_provides_fallback(self):
        ''' should fallback on the first one '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_headers']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})

        def test_versions_since_0_9_0_till_0_10_0():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'application/x-foo, application/xml'}}
            fun = self.funs['show_provides_old']
            self.qs.send(['show_doc', fun, doc, req])
            resp = self.qs.recv()
            # This is expected output, but not real because when json object
            # had been decoded to Python dict with losing of keys order
            # self.assertEqual('application/xml', resp['headers']['Content-Type'])
            # self.assertEqual(resp['body'], '<root><doc id="couch" /></root>')
            self.assertEqual('application/x-foo', resp['headers']['Content-Type'])
            self.assertEqual(resp['body'], 'foo? bar! bar!')

        def test_versions_since_0_10_0_till_0_11_0():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'application/x-foo, application/xml'}}
            fun = self.funs['show_provides']
            self.qs.send(['show', fun, doc, req])
            token, resp = self.qs.recv()
            self.assertEqual(token, 'resp')
            self.assertEqual('application/xml', resp['headers']['Content-Type'])
            self.assertEqual(resp['body'], '<root><doc id="couch" /></root>')

        def test_for_0_11_0_version_and_later():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'application/x-foo, application/xml'}}
            fun = self.funs['show_provides']
            ddoc = make_ddoc(['shows', 'provides'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'provides'], [doc, req])
            token, resp = self.qs.recv()
            self.assertEqual(token, 'resp')
            self.assertEqual('application/xml', resp['headers']['Content-Type'])
            self.assertEqual(resp['body'], '<root><doc id="couch" /></root>')

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_show_provides_mismatch(self):
        ''' should provides mime matcher without a match '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_headers']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})

        def test_versions_since_0_9_0_till_0_10_0():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'text/monkeys'}}
            fun = self.funs['show_provides_old']
            self.qs.send(['show_doc', fun, doc, req])
            resp = self.qs.recv()
            # that would be true if couchdb query server have been requested
            # directly, not from this test emulator
            # self.assertTrue('text/html' in resp['headers']['Content-Type'])
            self.assertEqual(resp['body'], '<html><body>couch</body></html>')

        def test_versions_since_0_10_0_till_0_11_0():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'text/monkeys'}}
            fun = self.funs['show_provides']
            self.qs.send(['show', fun, doc, req])
            resp = self.qs.recv()
            self.assertEqual(resp['error'], 'not_acceptable')

        def test_for_0_11_0_version_and_later():
            doc = {'_id': 'couch'}
            req = {'headers': {'Accept': 'text/monkeys'}}
            fun = self.funs['show_provides']
            ddoc = make_ddoc(['shows', 'provides'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'provides'], [doc, req])
            resp = self.qs.recv()
            self.assertEqual(resp[:2], ['error', 'not_acceptable'])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_missing_show_function(self):
        def test_versions_before_0_11_0():
            fun = self.funs['show_provides']
            ddoc = make_ddoc(['shows', 'provides'], fun)
            resp = self.qs.teach_ddoc(ddoc)
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command ddoc'})
            self.assertEqual(self.qs.close(), 1)

        def test_for_0_11_0_version_and_later():
            fun = self.funs['show_provides']
            ddoc = make_ddoc(['shows', 'provides'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'something', 'else'], [{}, {}])
            resp = self.qs.recv()
            self.assertTrue(isinstance(resp, list))
            error, type, message = resp
            self.assertEqual(error, 'error')
            self.assertEqual(type, 'not_found')
            self.assertEqual(self.qs.close(), 0)

        if COUCHDB_VERSION < (0, 11, 0):
            test_versions_before_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

class ErrorTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def error_in_show(doc, req):
            raise Error('show_error', 'testing')

        return locals()

    def test_recoverable_error_in_show_should_not_exit(self):
        ''' should not exit '''

        def test_versions_before_0_9_0():
            fun = self.funs['error_in_show']
            self.qs.send(['show_doc', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['error_in_show']
            self.qs.send(['show_doc', fun, {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'show_error', 'reason': 'testing'})
            self.assertTrue(self.qs.run(['reset']))

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['error_in_show']
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'show_error', 'reason': 'testing'})
            self.assertTrue(self.qs.run(['reset']))

        def test_for_0_11_0_version_and_later():
            fun = self.funs['error_in_show']
            ddoc = make_ddoc(['shows', 'error'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'error'],
                              [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['error', 'error_key', 'testing'])
            self.assertTrue(self.qs.run(['reset']))

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()


class FilterTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def emit_foo(doc):
            if doc['_id'] == 'foo':
                yield doc['_id'], None

        def filter_basic(doc, req, userctx):
            if doc.get('good', False):
                return True

        def filter_basic_new(doc, req):
            if doc.get('good', False):
                return True

        return locals()

    def test_changes_filter(self):
        ''' should only return true for good docs '''
        def test_version_before_0_10_0():
            fun = self.funs['filter_basic']
            self.qs.reset()
            self.assertEqual(self.qs.add_fun(fun), True)
            self.qs.send(['filter', [{'key': 'bam', 'good': True},
                         {'foo': 'bar'}, {'good': True}], {'req': 'foo'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command filter'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['filter_basic']
            self.qs.reset()
            self.assertEqual(self.qs.add_fun(fun), True)
            self.qs.send(['filter', [{'key': 'bam', 'good': True},
                         {'foo': 'bar'}, {'good': True}], {'req': 'foo'}])
            resp = self.qs.recv()
            self.assertEqual(resp, [True, [True, False, True]])

        def test_versions_since_0_11_0_till_0_11_1():
            fun = self.funs['filter_basic']
            ddoc = make_ddoc(['filters', 'basic'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['filters', 'basic'],
                              [[{'key': 'bam', 'good': True}, {'foo': 'bar'},
                              {'good': True}], {'req': 'foo'}])
            resp = self.qs.recv()
            self.assertEqual(resp, [True, [True, False, True]])

        def test_versions_for_0_11_1_and_later():
            fun = self.funs['filter_basic_new']
            ddoc = make_ddoc(['filters', 'basic'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['filters', 'basic'],
                              [[{'key': 'bam', 'good': True}, {'foo': 'bar'},
                              {'good': True}], {'req': 'foo'}])
            resp = self.qs.recv()
            self.assertEqual(resp, [True, [True, False, True]])

        if COUCHDB_VERSION < (0, 10, 0):
            test_version_before_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        elif COUCHDB_VERSION < (0, 11, 1):
            test_versions_since_0_11_0_till_0_11_1()
        else:
            test_versions_for_0_11_1_and_later()

    def test_filter_view(self):
        ''' should only return true for docs emited by view map '''
        def test_versions_before_0_11_0():
            ddoc = make_ddoc(['views', 'emit_foo'], self.funs['emit_foo'])
            resp = self.qs.teach_ddoc(ddoc)
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command ddoc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_11_0_till_trunk():
            ddoc = make_ddoc(['views', 'emit_foo'], self.funs['emit_foo'])
            resp = self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['views', 'emit_foo'], [[{'_id': 'foo'},
                                                             {'_id': 'bar'}]])
            resp = self.qs.recv()
            self.assertEqual(resp, ['error', 'unknown_command',
                                    'unknown ddoc command `views`'])
            self.assertEqual(self.qs.close(), 1)

        def test_for_trunk():
            ddoc = make_ddoc(['views', 'emit_foo'], self.funs['emit_foo'])
            resp = self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['views', 'emit_foo'], [[{'_id': 'foo'},
                                                             {'_id': 'bar'}]])
            resp = self.qs.recv()
            self.assertEqual(resp, [True, [True, False]])

        if COUCHDB_VERSION < (0, 11, 0):
            test_versions_before_0_11_0()
        elif COUCHDB_VERSION < TRUNK:
            test_versions_since_0_11_0_till_trunk()
        else:
            test_for_trunk()


class UpdateTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def update_basic(doc, req):
            doc['world'] = 'hello'
            return [doc, 'hello, doc']

        return locals()

    def test_update(self):
        ''' should return a doc and a resp body '''
        def test_version_before_0_10_0():
            fun = self.funs['update_basic']
            self.qs.reset()
            self.qs.send(['update', fun, {'foo': 'gnarly'}, {'method': 'POST'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command update'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['update_basic']
            self.qs.reset()
            self.qs.send(['update', fun, {'foo': 'gnarly'}, {'method': 'POST'}])
            up, doc, resp = self.qs.recv()
            self.assertEqual(up, 'up')
            self.assertEqual(doc, {'foo': 'gnarly', 'world': 'hello'})
            self.assertEqual(resp['body'], 'hello, doc')

        def test_versions_for_0_11_0_and_later():
            fun = self.funs['update_basic']
            ddoc = make_ddoc(['updates', 'basic'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['updates', 'basic'],
                                    [{'foo': 'gnarly'}, {'method': 'POST'}])
            up, doc, resp = self.qs.recv()
            self.assertEqual(up, 'up')
            self.assertEqual(doc, {'foo': 'gnarly', 'world': 'hello'})
            self.assertEqual(resp['body'], 'hello, doc')

        if COUCHDB_VERSION < (0, 10, 0):
            test_version_before_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_versions_for_0_11_0_and_later()

class ListTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def list_simple(head, req):
            send('first chunk')
            send(req['q'])
            for row in get_row():
                send(row['key'])
            return 'early'

        def list_simple_old(head, row, req, info):
            if head:
                return {'headers': {'Content-Type': 'text/plain'},
                        'code': 200,
                        'body': 'foo'}
            if row:
                return row['value']
            return 'tail'

        def show_sends(doc, req):
            start({'headers':{'Content-Type': 'text/plain'}})
            send('first chunk')
            send('second "chunk"')
            return 'tail'

        def show_while_get_rows(head, req):
            send('first chunk')
            send(req['q'])
            log('about to get_row: %s' % type(get_row))
            for row in get_row():
                send(row['key'])
            return 'tail'

        def show_while_get_rows_multi_send(head, req):
            send('bacon')
            log('about to get_row: %s' % type(get_row))
            for row in get_row():
                send(row['key'])
                send('eggs')
            return 'tail'

        def list_chunky(head, req):
            send('first chunk')
            send(req['q'])
            i = 0
            for row in get_row():
                send(row['key'])
                i += 1
                if i > 2:
                    return 'early tail'

        def list_with_headers(head, row, req, info):
            if head:
                return {'headers': {'Content-Type': 'text/plain'},
                        'code': 200,
                        'body': 'foo'}
            if row:
                return 'some "text" here'
            return 'tail'

        def list_with_rows(head, row, req, info):
            if head:
                return {'headers': {'Content-Type': 'text/plain'},
                        'code': 200,
                        'body': 'foo'}
            if row:
                return 'row value: ' + row['value']
            return 'tail ' + req['q']

        def list_capped_old(head, row, req, info):
            if head:
                return {'headers': {'Content-Type': 'text/plain'},
                        'code': 200,
                        'body': 'bacon'}
            i = 0
            for row in get_row():
                send(row['key'])
                i += 1
                if i > 2:
                    return 'early'

        def list_capped(head, req):
            send('bacon')
            i = 0
            for row in get_row():
                send(row['key'])
                i += 1
                if i > 2:
                    return 'early'

        def list_raw(head, req):
            send('first chunk')
            send(req['q'])
            for row in get_row():
                send(row['key'])
            return 'tail'

        return locals()

    def setUp(self):
        super(ListTestCase, self).setUp()
        def setUp_for_0_11_0_and_later():
            self.ddoc = {
                '_id': 'foo',
                'lists':{
                    'simple': self.funs['list_simple'],
                    'headers': self.funs['show_sends'],
                    'rows': self.funs['show_while_get_rows'],
                    'buffer_chunks': self.funs['show_while_get_rows_multi_send'],
                    'chunky': self.funs['list_chunky']
                }
            }
            self.qs.teach_ddoc(self.ddoc)

        if COUCHDB_VERSION >= (0, 11, 0):
            setUp_for_0_11_0_and_later()

    def test_list(self):
        ''' should run normal '''
        def test_versions_before_0_9_0():
            fun = self.funs['list_simple_old']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_begin'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['list_simple_old']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'headers': {'Content-Type': 'text/plain'},
                                   'code': 200, 'body': 'foo'})
            resp = self.qs.run(['list_row', {'value': 'bar'}, {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'bar'})
            resp = self.qs.run(['list_row', {'value': 'baz'}, {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'baz'})
            resp = self.qs.run(['list_row', {'value': 'bam'}, {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'bam'})
            resp = self.qs.run(['list_tail', {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'tail'})

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['list_simple']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam']])
            resp = self.qs.run(['list_row', {'key': 'foom'}])
            self.assertEqual(resp, ['chunks', ['foom']])
            resp = self.qs.run(['list_row', {'key': 'fooz'}])
            self.assertEqual(resp, ['chunks', ['fooz']])
            resp = self.qs.run(['list_row', {'key': 'foox'}])
            self.assertEqual(resp, ['chunks', ['foox']])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['early']])

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['lists', 'simple'],
                                         [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam']])
            resp = self.qs.run(['list_row', {'key': 'foom'}])
            self.assertEqual(resp, ['chunks', ['foom']])
            resp = self.qs.run(['list_row', {'key': 'fooz'}])
            self.assertEqual(resp, ['chunks', ['fooz']])
            resp = self.qs.run(['list_row', {'key': 'foox'}])
            self.assertEqual(resp, ['chunks', ['foox']])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['early']])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_headers(self):
        '''  should send head, row and tail '''
        def test_versions_before_0_9_0():
            fun = self.funs['list_with_headers']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'total_rows': 100500}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_begin'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['list_with_headers']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'total_rows': 100500}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'headers': {'Content-Type': 'text/plain'},
                                   'code': 200, 'body': 'foo'})
            resp = self.qs.run(['list_row', {'foo': 'bar'}, {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'some "text" here'})
            resp = self.qs.run(['list_tail', {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'tail'})

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_sends']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'total_rows': 100500}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'second "chunk"'],
                             {'headers': {'Content-Type': 'text/plain'}}])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['tail']])

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['lists', 'headers'],
                                         [{'total_rows': 100500}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'second "chunk"'],
                             {'headers': {'Content-Type': 'text/plain'}}])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['tail']])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_with_rows(self):
        '''  should render rows '''
        def test_versions_before_0_9_0():
            fun = self.funs['list_with_rows']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'total_rows': 100500}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_begin'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['list_with_rows']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'total_rows': 100500}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'headers': {'Content-Type': 'text/plain'},
                                   'code': 200, 'body': 'foo'})
            resp = self.qs.run(['list_row', {'value': 'bar'}, {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'row value: bar'})
            resp = self.qs.run(['list_row', {'value': 'baz'}, {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'row value: baz'})
            resp = self.qs.run(['list_row', {'value': 'bam'}, {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'row value: bam'})
            resp = self.qs.run(['list_tail', {'q': 'ok'}])
            self.assertEqual(resp, {'body': 'tail ok'})

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_while_get_rows']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam']])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['tail']])

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['lists', 'rows'],
                                         [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam']])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['tail']])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_buffer_multiple_chunks_should_buffer_em(self):
        ''' should buffer em '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_while_get_rows_multi_send']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_begin'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            self.fail('Undefined test case for version %s'
                      % '.'.join(map(str, COUCHDB_VERSION)))

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_while_get_rows_multi_send']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['bacon'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz', 'eggs']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam', 'eggs']])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['tail']])

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['lists', 'buffer_chunks'],
                                         [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['bacon'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz', 'eggs']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam', 'eggs']])
            resp = self.qs.run(['list_end'])
            self.assertEqual(resp, ['end', ['tail']])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_end_after_two(self):
        ''' should end after 2 '''
        def test_versions_before_0_9_0():
            fun = self.funs['list_chunky']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_begin'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            self.fail('Undefined test case for version %s'
                      % '.'.join(map(str, COUCHDB_VERSION)))

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['list_chunky']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam']])
            resp = self.qs.run(['list_row', {'key': 'foom'}])
            self.assertEqual(resp, ['end', ['foom', 'early tail']])
            resp = self.qs.run(['reset'])
            self.assertEqual(resp, True, [resp, type(resp)])

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['lists', 'chunky'],
                                         [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'bam'}])
            self.assertEqual(resp, ['chunks', ['bam']])
            resp = self.qs.run(['list_row', {'key': 'foom'}])
            self.assertEqual(resp, ['end', ['foom', 'early tail']])
            resp = self.qs.run(['reset'])
            self.assertEqual(resp, True, [resp, type(resp)])

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

class CrushTestCase(QueryServerMixIn, TestFuncsMixIn):

    def functions(self):
        def list_capped(head, req):
            send('bacon')
            i = 0
            for row in get_row():
                send(row['key'])
                i += 1
                if i > 2:
                    return 'early'

        def list_raw(head, req):
            send('first chunk')
            send(req['q'])
            for row in get_row():
                send(row['key'])
            return 'tail'

        def show_fatal(doc, req):
            raise FatalError('error_key', 'testing')

        return locals()

    def setUp(self):
        super(CrushTestCase, self).setUp()
        def setUp_for_0_11_0_and_later():
            self.ddoc = {
                '_id': 'foo',
                'lists': {
                    'capped': self.funs['list_capped'],
                    'raw': self.funs['list_raw'],
                },
                'shows': {
                    'fatal': self.funs['show_fatal']
                }
            }
            self.qs.teach_ddoc(self.ddoc)

        if COUCHDB_VERSION >= (0, 11, 0):
            setUp_for_0_11_0_and_later()

    def test_exit_if_sends_too_many_rows(self):
        ''' should exit if erlang sends too many rows '''
        def test_versions_before_0_9_0():
            fun = self.funs['list_capped']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_begin'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            self.fail('Undefined test case for version %s'
                      % '.'.join(map(str, COUCHDB_VERSION)))

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['list_capped']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['bacon'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'foom'}])
            self.assertEqual(resp, ['chunks', ['foom']])
            resp = self.qs.run(['list_row', {'key': 'fooz'}])
            self.assertEqual(resp, ['end', ['fooz', 'early']])
            resp = self.qs.run(['list_row', {'key': 'foox'}])
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_row'})
            self.assertEqual(self.qs.close(), 1)

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['lists', 'capped'],
                                         [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['bacon'], {'headers': {}}])
            resp = self.qs.run(['list_row', {'key': 'baz'}])
            self.assertEqual(resp, ['chunks', ['baz']])
            resp = self.qs.run(['list_row', {'key': 'foom'}])
            self.assertEqual(resp, ['chunks', ['foom']])
            resp = self.qs.run(['list_row', {'key': 'fooz'}])
            self.assertEqual(resp, ['end', ['fooz', 'early']])
            resp = self.qs.run(['list_row', {'key': 'foox'}])
            self.assertEqual(resp, ['error', 'unknown_command',
                                    'unknown command list_row'])
            self.assertEqual(self.qs.close(), 1)

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_exit_if_gets_non_row_in_the_middle(self):
        ''' should exit if it gets a non-row in the middle '''
        def test_versions_before_0_9_0():
            fun = self.funs['list_raw']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list_begin', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_begin'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            self.fail('Undefined test case for version %s'
                      % '.'.join(map(str, COUCHDB_VERSION)))

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['list_raw']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['reset'])
            self.assertEqual(resp['error'], 'list_error')
            self.assertEqual(self.qs.close(), 1)

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['lists', 'raw'],
                                         [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
            resp = self.qs.run(['reset'])
            self.assertEqual(resp[0], 'error')
            self.assertEqual(resp[1], 'list_error')
            self.assertEqual(self.qs.close(), 1)

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

    def test_fatal(self):
        ''' should exit '''
        def test_versions_before_0_9_0():
            fun = self.funs['show_fatal']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['show_doc', {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command show_doc'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_9_0_till_0_10_0():
            fun = self.funs['show_fatal']
            self.qs.send(['show_doc', fun, {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'error_key', 'reason': 'testing'})
            self.assertEqual(self.qs.close(), 1)

        def test_versions_since_0_10_0_till_0_11_0():
            fun = self.funs['show_fatal']
            self.qs.send(['show', fun, {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'error_key', 'reason': 'testing'})
            self.assertEqual(self.qs.close(), 1)

        def test_for_0_11_0_version_and_later():
            self.qs.send_ddoc(self.ddoc, ['shows', 'fatal'],
                                         [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['error', 'error_key', 'testing'])
            self.assertEqual(self.qs.close(), 1)

        if COUCHDB_VERSION < (0, 9, 0):
            test_versions_before_0_9_0()
        elif COUCHDB_VERSION < (0, 10, 0):
            test_versions_since_0_9_0_till_0_10_0()
        elif COUCHDB_VERSION < (0, 11, 0):
            test_versions_since_0_10_0_till_0_11_0()
        else:
            test_for_0_11_0_version_and_later()

class LegacyTestCases(unittest.TestCase):

    def test_reset(self):
        input = StringIO('["reset"]\n')
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        self.assertEquals(output.getvalue(), 'true\n')

    def test_add_fun(self):
        input = StringIO('["add_fun", "def fun(doc): yield None, doc"]\n')
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        self.assertEquals(output.getvalue(), 'true\n')

    def test_map_doc(self):
        input = StringIO('["add_fun", "def fun(doc): yield None, doc"]\n'
                         '["map_doc", {"foo": "bar"}]\n')
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        self.assertEqual(output.getvalue(),
                         'true\n'
                         '[[[null, {"foo": "bar"}]]]\n')

    def test_i18n(self):
        input = StringIO('["add_fun", "def fun(doc): yield doc[\\"test\\"], doc"]\n'
                         '["map_doc", {"test": "b\xc3\xa5r"}]\n')
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        self.assertEqual(output.getvalue(),
                         'true\n'
                         '[[["b\xc3\xa5r", {"test": "b\xc3\xa5r"}]]]\n')


    def test_map_doc_with_logging(self):
        fun = 'def fun(doc): log(\'running\'); yield None, doc'
        input = StringIO('["add_fun", "%s"]\n'
                         '["map_doc", {"foo": "bar"}]\n' % fun)
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        if (0, 9, 0) <= COUCHDB_VERSION < (0, 11, 0):
            self.assertEqual(output.getvalue(), 'true\n'
                                                '{"log": "running"}\n'
                                                '[[[null, {"foo": "bar"}]]]\n')
        elif COUCHDB_VERSION >= (0, 11, 0):
            self.assertEqual(output.getvalue(), 'true\n'
                                                '["log", "running"]\n'
                                                '[[[null, {"foo": "bar"}]]]\n')

    def test_map_doc_with_logging_json(self):
        fun = 'def fun(doc): log([1, 2, 3]); yield None, doc'
        input = StringIO('["add_fun", "%s"]\n'
                         '["map_doc", {"foo": "bar"}]\n' % fun)
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        if COUCHDB_VERSION < (0, 11, 0):
            self.assertEqual(output.getvalue(), 'true\n'
                                                '{"log": "[1, 2, 3]"}\n'
                                                '[[[null, {"foo": "bar"}]]]\n')
        else:
            self.assertEqual(output.getvalue(), 'true\n'
                                                '["log", "[1, 2, 3]"]\n'
                                                '[[[null, {"foo": "bar"}]]]\n')

    def test_reduce(self):
        input = StringIO('["reduce", '
                          '["def fun(keys, values): return sum(values)"], '
                          '[[null, 1], [null, 2], [null, 3]]]\n')
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        self.assertEqual(output.getvalue(), '[true, [6]]\n')

    def test_reduce_with_logging(self):
        input = StringIO('["reduce", '
                          '["def fun(keys, values): log(\'Summing %r\''
                          ' % (values,)); return sum(values)"], '
                          '[[null, 1], [null, 2], [null, 3]]]\n')
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        if COUCHDB_VERSION < (0, 11, 0):
            self.assertEqual(output.getvalue(), '{"log": "Summing (1, 2, 3)"}\n'
                                                '[true, [6]]\n')
        else:
            self.assertEqual(output.getvalue(), '["log", "Summing (1, 2, 3)"]\n'
                                                '[true, [6]]\n')

    def test_rereduce(self):
        input = StringIO('["rereduce", '
                          '["def fun(keys, values, rereduce): return sum(values)"], '
                          '[1, 2, 3]]\n')
        output = StringIO()
        view.run(input=input, output=output, version=COUCHDB_VERSION)
        self.assertEqual(output.getvalue(), '[true, [6]]\n')

def suite():
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocTestSuite(view))
    # official tests
    suite.addTest(unittest.makeSuite(ViewTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ValidateTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ShowTestCase, 'test'))
    suite.addTest(unittest.makeSuite(FilterTestCase, 'test'))
    suite.addTest(unittest.makeSuite(UpdateTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ListTestCase, 'test'))
    suite.addTest(unittest.makeSuite(CrushTestCase, 'test'))
    # old tests
    suite.addTest(unittest.makeSuite(LegacyTestCases, 'test'))
    return suite


if __name__ == '__main__':
    versions = [
        (0, 8, 0),
        (0, 9, 0),
        (0, 10, 0),
        (0, 11, 0), (0, 11, 1),
        (1, 0 ,0),
        (1, 1 ,0),
        TRUNK
    ]
    for version in versions:
        print >> sys.stderr, \
                 'Running for version:', \
                 ['.'.join(map(str, version)), 'trunk'][version == TRUNK]
        COUCHDB_VERSION = version
        TestRunner(defaultTest='suite')
        print >> sys.stderr, '\n'
