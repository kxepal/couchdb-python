# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2008 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
import sys
import doctest
import unittest
from couchdb import view
from couchdb.util import CouchDBVersion
from couchdb.tests.testutil import QueryServer
from StringIO import StringIO
import inspect
import textwrap

from os.path import normpath, join, dirname
VIEW_SERVER = normpath(join(dirname(__file__), '../view.py'))

def funcs():
    def emit_twise(doc):
        yield 'foo', doc['a']
        yield 'bar', doc['a']

    def emit_once(doc):
        yield 'baz', doc['a']

    def reduce_values_length(keys, values, rereduce):
        return len(values)

    def reduce_values_sum(keys, values, rereduce):
        return sum(values)

    def validate_forbidden(newdoc, olddoc, userctx):
        if newdoc.get('bad', False):
            raise Forbidden('bad doc')

    def show_simple(doc, req):
        log('ok')
        return ' - '.join([doc['title'], doc['body']])

    def show_headers(doc, req):
        resp = {'code': 200, 'headers': {'X-Couchdb-Python': 'Hello, world!'}}
        resp['body'] = ' - '.join([doc['title'], doc['body']])
        return resp

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

    def list_simple(head, req):
        send('first chunk')
        send(req['q'])
        for row in get_row():
            send(row['key'])
        return 'early'

    def list_chunky(head, req):
        send('first chunk')
        send(req['q'])
        i = 0
        for row in get_row():
            send(row['key'])
            i += 1
            if i > 2:
                return 'early tail'

    def list_old_style(head, row, req, info):
        return 'stuff'

    #@CouchDBVersion.minimal(0, 9, 0)
    def list_with_headers(head, row, req, info):
        if head:
            return {'headers': {'Content-Type': 'text/plain'},
                    'code': 200,
                    'body': 'foo'}
        if row:
            return 'some "text" here'
        return 'tail'

    #@CouchDBVersion.minimal(0, 9, 0)
    def list_with_rows(head, row, req, info):
        if head:
            return {'headers': {'Content-Type': 'text/plain'},
                    'code': 200,
                    'body': 'foo'}
        if row:
            return 'row value: ' + row['value']
        return 'tail ' + req['q']

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

    def filter_basic(doc, req, userctx):
        if doc.get('good', False):
            return True

    def update_basic(doc, req):
        doc['world'] = 'hello'
        return [doc, 'hello, doc']

    def internal(doc, req):
        1/0

    def error(doc, req):
        raise Error('error_key', 'testing')

    def fatal(doc, req):
        raise FatalError('error_key', 'testing')

    d = locals()
    for k, f in d.items():
        d[k] = textwrap.dedent(inspect.getsource(f))
    return d
functions = funcs()


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
        self.qs = QueryServer(VIEW_SERVER, version=CouchDBVersion.current)

    def tearDown(self):
        self.qs.close()

class ViewTestCase(QueryServerMixIn):

    def test_reset(self):
        ''' should reset '''
        resp = self.qs.run(['reset'])
        self.assertEquals(resp, True)

    def test_reset_should_not_erase_ddocs(self):
        ''' should not erase ddocs on reset '''

        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            ''' ddocs had been introduced in 0.11.0 '''

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            fun = functions['show_simple']
            ddoc = make_ddoc(['shows', 'simple'], fun)
            self.qs.teach_ddoc(ddoc)
            self.assertEqual(self.qs.run(['reset']), True)
            self.qs.send_ddoc(ddoc, ['shows', 'simple'],
                              [{'title': 'best ever', 'body': 'doc body'}, {}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['resp', {'body': 'best ever - doc body'}])
        test()

    def test_run_map_funs(self):
        ''' should run map funs '''
        self.qs.reset()
        self.assertEqual(self.qs.run(['add_fun', functions['emit_twise']]), True)
        self.assertEqual(self.qs.run(['add_fun', functions['emit_once']]), True)
        rows = self.qs.run(['map_doc', {'_id': 'test_doc', 'a': 'b'}])
        self.assertEqual(rows[0][0], ['foo', 'b'])
        self.assertEqual(rows[0][1], ['bar', 'b'])
        self.assertEqual(rows[1][0], ['baz', 'b'])

    def test_reduce(self):
        ''' should reduce '''
        fun = functions['reduce_values_length']
        self.qs.reset()
        kvs = [(i, i*2) for i in xrange(10)]
        self.assertEqual(self.qs.run(['reduce', [fun], kvs]), [True, [10]])

    def test_rereduce(self):
        ''' should rereduce '''
        fun = functions['reduce_values_sum']
        self.qs.reset()
        resp = self.qs.run(['rereduce', [fun], range(10)])
        self.assertEqual(resp, [True, [45]])

    def test_learn_design_docs(self):
        ''' should learn design docs '''
        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            # ddocs had been introduced in 0.11.0
            pass

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            ddoc = {'_id': 'foo'}
            self.qs.reset()
            self.assertEqual(self.qs.teach_ddoc(ddoc), True)
        test()

class ValidateTestCase(QueryServerMixIn):

    def setUp(self):
        super(ValidateTestCase, self).setUp()
        @CouchDBVersion.minimal(0, 9, 0)
        def setup():
            self.qs.reset()

        @CouchDBVersion.minimal(0, 11, 0)
        def setup():
            fun = functions['validate_forbidden']
            self.ddoc = make_ddoc(['validate_doc_update'], fun)
            self.qs.teach_ddoc(self.ddoc)
        setup()


    def test_validate_all_good_updates(self):
        ''' should allow good updates '''
        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            fun = functions['validate_forbidden']
            self.qs.send(['validate', fun, {'good': True}, {}, {}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc, ['validate_doc_update'],
                                         [{'good': True}, {}, {}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, 1)

    def test_validate_reject_invalid_updates(self):
        ''' should reject invalid updates '''
        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            fun = functions['validate_forbidden']
            self.qs.send(['validate', fun, {'bad': True}, {}, {}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc, ['validate_doc_update'],
                                         [{'bad': True}, {}, {}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, {'forbidden': 'bad doc'})

class ShowTestCase(QueryServerMixIn):

    @CouchDBVersion.minimal(0, 9, 0)
    def test_show(self):
        ''' should show '''
        fun = functions['show_simple']
        self.qs.send(['show_doc', fun,
                     {'title': 'best ever', 'body': 'doc body'}])
        resp = self.qs.recv()
        self.assertEqual(resp, {'body': 'best ever - doc body'})

    @CouchDBVersion.minimal(0, 10, 0)
    def test_show(self):
        ''' should show '''

        fun = functions['show_simple']
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            ddoc = make_ddoc(['shows', 'simple'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'simple'],
                              [{'title': 'best ever', 'body': 'doc body'}, {}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['resp', {'body': 'best ever - doc body'}])

    @CouchDBVersion.minimal(0, 9, 0)
    def test_show_with_headers(self):
        ''' should show headers '''
        fun = functions['show_headers']
        self.qs.send(['show_doc', fun,
                     {'title': 'best ever', 'body': 'doc body'}])
        resp = self.qs.recv()
        self.assertEqual(resp, {'headers': {'X-Couchdb-Python': 'Hello, world!'},
                        'code': 200,
                        'body': 'best ever - doc body'})

    @CouchDBVersion.minimal(0, 10, 0)
    def test_show_with_headers(self):
        ''' should show headers '''
        fun = functions['show_headers']
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            self.qs.send(['show', fun,
                         {'title': 'best ever', 'body': 'doc body'}, {}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            ddoc = make_ddoc(['shows', 'headers'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'headers'],
                              [{'title': 'best ever', 'body': 'doc body'}, {}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['resp',
                {'code': 200, 'headers': {'X-Couchdb-Python': 'Hello, world!'},
                 'body': 'best ever - doc body'}])

class ErrorTestCase(QueryServerMixIn):

    def test_recoverable_error_should_not_exit(self):
        ''' should not exit '''
        fun = functions['error']

        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            self.qs.send(['show_doc', fun, {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            self.qs.send(['show', fun, {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            ddoc = make_ddoc(['shows', 'error'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['shows', 'error'],
                              [{'foo': 'bar'}, {'q': 'ok'}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['error', 'error_key', 'testing'])
        self.qs.run(['reset']) == True


class FilterTestCase(QueryServerMixIn):

    @CouchDBVersion.minimal(0, 9, 0)
    def test_changes_filter(self):
        ''' should only return true for good docs '''
        # filters are introduced in 0.10.0

    @CouchDBVersion.minimal(0, 10, 0)
    def test_changes_filter(self):
        ''' should only return true for good docs '''
        fun = functions['filter_basic']

        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            self.qs.reset()
            self.assertEqual(self.qs.add_fun(fun), True)
            self.qs.send(['filter', [{'key': 'bam', 'good': True},
                         {'foo': 'bar'}, {'good': True}], {'req': 'foo'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            ddoc = make_ddoc(['filters', 'basic'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc, ['filters', 'basic'],
                                    [[{'key': 'bam', 'good': True},
                                    {'foo': 'bar'}, {'good': True}],
                                    {'req': 'foo'}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, [True, [True, False, True]], resp)


class UpdateTestCase(QueryServerMixIn):

    @CouchDBVersion.minimal(0, 9, 0)
    def test_update(self):
        # updates had been introduced in 0.10.0
        pass

    @CouchDBVersion.minimal(0, 10, 0)
    def test_update(self):
        ''' should return a doc and a resp body '''
        fun = functions['update_basic']

        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            self.qs.reset()
            self.qs.send(['update', fun,
                         {'foo': 'gnarly'}, {'method': 'POST'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            ddoc = make_ddoc(['updates', 'basic'], fun)
            self.qs.teach_ddoc(ddoc)
            self.qs.send_ddoc(ddoc,
                              ['updates', 'basic'],
                              [{'foo': 'gnarly'}, {'method': 'POST'}])

        test()
        up, doc, resp = self.qs.recv()
        self.assertEqual(up, 'up')
        self.assertEqual(doc, {'foo': 'gnarly', 'world': 'hello'})
        self.assertEqual(resp['body'], 'hello, doc')

class ListTestCase(QueryServerMixIn):

    def setUp(self):
        super(ListTestCase, self).setUp()

        @CouchDBVersion.minimal(0, 9, 0)
        def setup():
            pass

        @CouchDBVersion.minimal(0, 11, 0)
        def setup():
            self.ddoc = {
                '_id': 'foo',
                'lists':{
                    'simple': functions['list_simple'],
                    'headers': functions['show_sends'],
                    'rows': functions['show_while_get_rows'],
                    'buffer_chunks': functions['show_while_get_rows_multi_send'],
                    'chunky': functions['list_chunky']
                }
            }
            self.qs.teach_ddoc(self.ddoc)
        setup()

    def test_list(self):
        '''should run normal'''

        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['list_simple']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc,
                              ['lists', 'simple'],
                              [{'foo': 'bar'}, {'q': 'ok'}])
        test()
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

    @CouchDBVersion.minimal(0, 9, 0)
    def test_headers(self):
        '''  should send head, row and tail '''
        fun = functions['list_with_headers']
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


    @CouchDBVersion.minimal(0, 10, 0)
    def test_headers(self):
        ''' should do headers proper '''
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['show_sends']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'total_rows': 100500}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc,
                              ['lists', 'headers'],
                              [{'total_rows': 100500}, {'q': 'ok'}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['start', ['first chunk', 'second "chunk"'],
                         {'headers': {'Content-Type': 'text/plain'}}])
        resp = self.qs.run(['list_end'])
        self.assertEqual(resp, ['end', ['tail']])

    @CouchDBVersion.minimal(0, 9, 0)
    def test_with_rows(self):
        '''  should render rows '''
        fun = functions['list_with_rows']
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

    @CouchDBVersion.minimal(0, 10, 0)
    def test_with_rows(self):
        ''' should list em '''
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['show_while_get_rows']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc,
                              ['lists', 'rows'],
                              [{'foo': 'bar'}, {'q': 'ok'}])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
        resp = self.qs.run(['list_row', {'key': 'baz'}])
        self.assertEqual(resp, ['chunks', ['baz']])
        resp = self.qs.run(['list_row', {'key': 'bam'}])
        self.assertEqual(resp, ['chunks', ['bam']])
        resp = self.qs.run(['list_end'])
        self.assertEqual(resp, ['end', ['tail']])


    def test_buffer_multiple_chunks_should_buffer_em(self):
        ''' should buffer em '''
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['show_while_get_rows_multi_send']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
         self.qs.send_ddoc(self.ddoc,
                                ['lists', 'buffer_chunks'],
                                [{'foo': 'bar'}, {'q': 'ok'}])

        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['start', ['bacon'], {'headers': {}}])
        resp = self.qs.run(['list_row', {'key': 'baz'}])
        self.assertEqual(resp, ['chunks', ['baz', 'eggs']])
        resp = self.qs.run(['list_row', {'key': 'bam'}])
        self.assertEqual(resp, ['chunks', ['bam', 'eggs']])
        resp = self.qs.run(['list_end'])
        self.assertEqual(resp, ['end', ['tail']])

    def test_end_after_two(self):
        ''' should end after 2 '''
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['list_chunky']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc,
                                    ['lists', 'chunky'],
                                    [{'foo': 'bar'}, {'q': 'ok'}])
        test()
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

class CrushTestCase(QueryServerMixIn):


    def setUp(self):
        super(CrushTestCase, self).setUp()
        @CouchDBVersion.minimal(0, 9, 0)
        def setup():
            pass

        @CouchDBVersion.minimal(0, 11, 0)
        def setup():
            self.ddoc = {
                '_id': 'foo',
                'lists': {
                    'capped': functions['list_capped'],
                    'raw': functions['list_raw'],
                },
                'shows': {
                    'fatal': functions['fatal']
                }
            }
            self.qs.teach_ddoc(self.ddoc)
        setup()


    @CouchDBVersion.minimal(0, 10, 0)
    def test_exit_if_sends_too_many_rows(self):
        ''' should exit if erlang sends too many rows '''
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['list_capped']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 10, 0)
        def handle_error():
            resp = self.qs.run(['list_row', {'key': 'foox'}])
            self.assertEqual(resp, {'error': 'unknown_command',
                                    'reason': 'unknown command list_row'})

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc,
                              ['lists', 'capped'],
                              [{'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def handle_error():
            resp = self.qs.run(['list_row', {'key': 'foox'}])
            self.assertEqual(resp, ['error',
                                    'unknown_command',
                                    'unknown command list_row'])
        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['start', ['bacon'], {'headers': {}}])
        resp = self.qs.run(['list_row', {'key': 'baz'}])
        self.assertEqual(resp, ['chunks', ['baz']])
        resp = self.qs.run(['list_row', {'key': 'foom'}])
        self.assertEqual(resp, ['chunks', ['foom']])
        resp = self.qs.run(['list_row', {'key': 'fooz'}])
        self.assertEqual(resp, ['end', ['fooz', 'early']])
        handle_error()
        self.qs.pipe.wait()
        self.assertEqual(self.qs.pipe.returncode, 1)


    @CouchDBVersion.minimal(0, 10, 0)
    def test_exit_if_gets_non_row_in_the_middle(self):
        ''' should exit if it gets a non-row in the middle '''
        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['list_raw']
            self.qs.reset()
            self.qs.add_fun(fun)
            self.qs.send(['list', {'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 10, 0)
        def handle_error():
            resp = self.qs.run(['reset'])
            self.assertEqual(resp['error'], 'list_error')

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc,
                              ['lists', 'raw'],
                              [{'foo': 'bar'}, {'q': 'ok'}])

        @CouchDBVersion.minimal(0, 11, 0)
        def handle_error():
            resp = self.qs.run(['reset'])
            self.assertEqual(resp[0], 'error')
            self.assertEqual(resp[1], 'list_error')

        test()
        resp = self.qs.recv()
        self.assertEqual(resp, ['start', ['first chunk', 'ok'], {'headers': {}}])
        handle_error()
        self.assertEqual(self.qs.close(), 1, self.qs.returncode)

    def test_fatal(self):
        '''should exit'''

        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            fun = functions['fatal']
            self.qs.send(['show_doc', fun, {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'error_key', 'reason': 'testing'})

        @CouchDBVersion.minimal(0, 10, 0)
        def test():
            fun = functions['fatal']
            self.qs.send(['show', fun, {'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, {'error': 'error_key', 'reason': 'testing'})

        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.qs.send_ddoc(self.ddoc,
                                    ['shows', 'fatal'],
                                    [{'foo': 'bar'}, {'q': 'ok'}])
            resp = self.qs.recv()
            self.assertEqual(resp, ['error', 'error_key', 'testing'])

        test()
        self.assertEqual(self.qs.close(), 1)


class LegacyTestCases(unittest.TestCase):


    def test_reset(self):
        input = StringIO('["reset"]\n')
        output = StringIO()
        view.run(input=input, output=output)
        self.assertEquals(output.getvalue(), 'true\n')

    def test_add_fun(self):
        input = StringIO('["add_fun", "def fun(doc): yield None, doc"]\n')
        output = StringIO()
        view.run(input=input, output=output)
        self.assertEquals(output.getvalue(), 'true\n')

    def test_map_doc(self):
        input = StringIO('["add_fun", "def fun(doc): yield None, doc"]\n'
                         '["map_doc", {"foo": "bar"}]\n')
        output = StringIO()
        view.run(input=input, output=output)
        self.assertEqual(output.getvalue(),
                         'true\n'
                         '[[[null, {"foo": "bar"}]]]\n')

    def test_i18n(self):
        input = StringIO('["add_fun", "def fun(doc): yield doc[\\"test\\"], doc"]\n'
                         '["map_doc", {"test": "b\xc3\xa5r"}]\n')
        output = StringIO()
        view.run(input=input, output=output)
        self.assertEqual(output.getvalue(),
                         'true\n'
                         '[[["b\xc3\xa5r", {"test": "b\xc3\xa5r"}]]]\n')


    def test_map_doc_with_logging(self):
        fun = 'def fun(doc): log(\'running\'); yield None, doc'
        input = StringIO('["add_fun", "%s"]\n'
                         '["map_doc", {"foo": "bar"}]\n' % fun)
        output = StringIO()
        view.run(input=input, output=output)
        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            self.assertEqual(output.getvalue(), 'true\n'
                                                '{"log": "running"}\n'
                                                '[[[null, {"foo": "bar"}]]]\n')
        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.assertEqual(output.getvalue(), 'true\n'
                                                '["log", "running"]\n'
                                                '[[[null, {"foo": "bar"}]]]\n')
        test()

    def test_map_doc_with_logging_json(self):
        fun = 'def fun(doc): log([1, 2, 3]); yield None, doc'
        input = StringIO('["add_fun", "%s"]\n'
                         '["map_doc", {"foo": "bar"}]\n' % fun)
        output = StringIO()
        view.run(input=input, output=output)
        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            self.assertEqual(output.getvalue(), 'true\n'
                                                '{"log": "[1, 2, 3]"}\n'
                                                '[[[null, {"foo": "bar"}]]]\n')
        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.assertEqual(output.getvalue(), 'true\n'
                                                '["log", "[1, 2, 3]"]\n'
                                                '[[[null, {"foo": "bar"}]]]\n')
        test()

    def test_reduce(self):
        input = StringIO('["reduce", '
                          '["def fun(keys, values): return sum(values)"], '
                          '[[null, 1], [null, 2], [null, 3]]]\n')
        output = StringIO()
        view.run(input=input, output=output)
        self.assertEqual(output.getvalue(), '[true, [6]]\n')

    def test_reduce_with_logging(self):
        input = StringIO('["reduce", '
                          '["def fun(keys, values): log(\'Summing %r\' % (values,)); return sum(values)"], '
                          '[[null, 1], [null, 2], [null, 3]]]\n')
        output = StringIO()
        view.run(input=input, output=output)
        @CouchDBVersion.minimal(0, 9, 0)
        def test():
            self.assertEqual(output.getvalue(), '{"log": "Summing (1, 2, 3)"}\n'
                                                '[true, [6]]\n')
        @CouchDBVersion.minimal(0, 11, 0)
        def test():
            self.assertEqual(output.getvalue(), '["log", "Summing (1, 2, 3)"]\n'
                                                '[true, [6]]\n')
        test()

    def test_rereduce(self):
        input = StringIO('["rereduce", '
                          '["def fun(keys, values, rereduce): return sum(values)"], '
                          '[1, 2, 3]]\n')
        output = StringIO()
        view.run(input=input, output=output)
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
    import types
    class TestRunner(unittest.main):
        def runTests(self):
            if self.testRunner is None:
                self.testRunner = unittest.TextTestRunner
            if isinstance(self.testRunner, (type, types.ClassType)):
                try:
                    testRunner = self.testRunner(verbosity=self.verbosity,
                                                 failfast=self.failfast,
                                                 buffer=self.buffer)
                except TypeError:
                    # didn't accept the verbosity, buffer or failfast arguments
                    testRunner = self.testRunner()
            else:
                # it is assumed to be a TestRunner instance
                testRunner = self.testRunner
            self.result = testRunner.run(self.test)
            # remove forced exit
            #if self.exit:
            #    sys.exit(not self.result.wasSuccessful())
    for version in [(0,9,0), (0, 10, 0), (0, 11, 0), (1, 0 ,0)]:
        print >>sys.stderr, 'Running for version:','.'.join(map(str, version))
        CouchDBVersion.current = version
        TestRunner(defaultTest='suite')
        print >>sys.stderr, '\n'
