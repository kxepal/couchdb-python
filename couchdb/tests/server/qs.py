# -*- coding: utf-8 -*-
#
import unittest
from cStringIO import StringIO
from couchdb.server import BaseQueryServer, SimpleQueryServer
from couchdb.server import exceptions
from couchdb.server.helpers import partial, wrap_func_to_ddoc

class BaseQueryServerTestCase(unittest.TestCase):

    def test_set_version(self):
        server = BaseQueryServer((1, 2, 3))
        self.assertEqual(server.version, (1, 2, 3))

    def test_set_latest_version_by_default(self):
        server = BaseQueryServer()
        self.assertEqual(server.version, (999, 999, 999))

    def test_set_config_option(self):
        server = BaseQueryServer(foo='bar')
        self.assertTrue('foo' in server.config)
        self.assertEqual(server.config['foo'], 'bar')

    def test_config_option_handler(self):
        class CustomServer(BaseQueryServer):
            def config_foo(self, value):
                self.config['baz'] = value
        server = CustomServer(foo='bar')
        self.assertTrue('foo' not in server.config)
        self.assertTrue('baz' in server.config)
        self.assertEqual(server.config['baz'], 'bar')

    def test_handle_fatal_error(self):
        def command_foo(*a, **k):
            raise exceptions.FatalError('foo', 'bar')
        def maybe_fatal_error(func):
            def wrapper(exc_type, exc_value, exc_traceback):
                assert exc_type is exceptions.FatalError
                return func(exc_type, exc_value, exc_traceback)
            return wrapper
        output = StringIO()
        server = BaseQueryServer(output=output)
        server.handle_fatal_error = maybe_fatal_error(server.handle_fatal_error)
        server.commands['foo'] = command_foo
        try:
            server.process_request(['foo', 'bar'])
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))

    def test_response_for_fatal_error_oldstyle(self):
        def command_foo(*a, **k):
            raise exceptions.FatalError('foo', 'bar')
        output = StringIO()
        server = BaseQueryServer(version=(0, 9, 0), output=output)
        server.commands['foo'] = command_foo
        try:
            server.process_request(['foo', 'bar'])
        except Exception:
            pass
        self.assertEqual(
            output.getvalue(),
            '{"reason": "bar", "error": "foo"}\n'
        )

    def test_response_for_fatal_error_newstyle(self):
        def command_foo(*a, **k):
            raise exceptions.Error('foo', 'bar')
        output = StringIO()
        server = BaseQueryServer(version=(0, 11, 0), output=output)
        server.commands['foo'] = command_foo
        try:
            server.process_request(['foo', 'bar'])
        except Exception:
            pass
        self.assertEqual(output.getvalue(), '["error", "foo", "bar"]\n')

    def test_handle_qs_error(self):
        def command_foo(*a, **k):
            raise exceptions.Error('foo', 'bar')
        def maybe_qs_error(func):
            def wrapper(exc_type, exc_value, exc_traceback):
                assert exc_type is exceptions.Error
                func.im_self.mock_last_error = exc_type
                return func(exc_type, exc_value, exc_traceback)
            return wrapper
        output = StringIO()
        server = BaseQueryServer(output=output)
        server.handle_qs_error = maybe_qs_error(server.handle_qs_error)
        server.commands['foo'] = command_foo
        server.process_request(['foo', 'bar'])

    def test_response_for_qs_error_oldstyle(self):
        def command_foo(*a, **k):
            raise exceptions.Error('foo', 'bar')
        output = StringIO()
        server = BaseQueryServer(version=(0, 9, 0), output=output)
        server.commands['foo'] = command_foo
        server.process_request(['foo', 'bar'])
        self.assertEqual(output.getvalue(), '{"reason": "bar", "error": "foo"}\n')

    def test_response_for_qs_error_newstyle(self):
        def command_foo(*a, **k):
            raise exceptions.Error('foo', 'bar')
        output = StringIO()
        server = BaseQueryServer(version=(0, 11, 0), output=output)
        server.commands['foo'] = command_foo
        server.process_request(['foo', 'bar'])
        self.assertEqual(output.getvalue(), '["error", "foo", "bar"]\n')

    def test_handle_forbidden_error(self):
        def command_foo(*a, **k):
            raise exceptions.Forbidden('foo')
        def maybe_forbidden_error(func):
            def wrapper(exc_type, exc_value, exc_traceback):
                assert exc_type is exceptions.Forbidden
                return func(exc_type, exc_value, exc_traceback)
            return wrapper
        output = StringIO()
        server = BaseQueryServer(output=output)
        server.handle_forbidden_error = maybe_forbidden_error(server.handle_forbidden_error)
        server.commands['foo'] = command_foo
        server.process_request(['foo', 'bar'])

    def test_response_for_forbidden_error(self):
        def command_foo(*a, **k):
            raise exceptions.Forbidden('foo')
        output = StringIO()
        server = BaseQueryServer(output=output)
        server.commands['foo'] = command_foo
        server.process_request(['foo', 'bar'])
        self.assertEqual(output.getvalue(), '{"forbidden": "foo"}\n')

    def test_handle_python_exception(self):
        def command_foo(*a, **k):
            raise ValueError('that was a typo')
        def maybe_py_error(func):
            def wrapper(exc_type, exc_value, exc_traceback):
                assert exc_type is ValueError
                return func(exc_type, exc_value, exc_traceback)
            return wrapper
        output = StringIO()
        server = BaseQueryServer(output=output)
        server.handle_python_exception = maybe_py_error(server.handle_python_exception)
        server.commands['foo'] = command_foo
        try:
            server.process_request(['foo', 'bar'])
        except Exception, err:
            self.assertTrue(isinstance(err, ValueError))

    def test_response_python_exception_oldstyle(self):
        def command_foo(*a, **k):
            raise ValueError('that was a typo')
        output = StringIO()
        server = BaseQueryServer(version=(0, 9, 0), output=output)
        server.commands['foo'] = command_foo
        try:
            server.process_request(['foo', 'bar'])
        except Exception:
            pass
        self.assertEqual(
            output.getvalue(),
            '{"reason": "that was a typo", "error": "ValueError"}\n'
        )

    def test_response_python_exception_newstyle(self):
        def command_foo(*a, **k):
            raise ValueError('that was a typo')
        output = StringIO()
        server = BaseQueryServer(version=(0, 11, 0), output=output)
        server.commands['foo'] = command_foo
        try:
            server.process_request(['foo', 'bar'])
        except Exception:
            pass
        self.assertEqual(
            output.getvalue(),
            '["error", "ValueError", "that was a typo"]\n'
        )

    def test_process_request(self):
        server = BaseQueryServer()
        server.commands['foo'] = lambda s, x: x == 42
        self.assertTrue(server.process_request(['foo', 42]))

    def test_process_request_ddoc(self):
        server = BaseQueryServer()
        server.commands['foo'] = lambda s, x: x == 42
        self.assertTrue(server.process_request(['foo', 42]))

    def test_pass_server_instance_to_command_handler(self):
        server = BaseQueryServer()
        server.commands['foo'] = lambda s, x: server is s
        self.assertTrue(server.process_request(['foo', 'bar']))

    def test_raise_fatal_error_on_unknown_command(self):
        server = BaseQueryServer(output=StringIO())
        try:
            server.process_request(['foo', 'bar'])
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'unknown_command')

    def test_receive(self):
        server = BaseQueryServer(input=StringIO('["foo"]\n{"bar": "baz"}\n'))
        self.assertEqual(list(server.receive()), [['foo'], {'bar': 'baz'}])

    def test_response(self):
        output = StringIO()
        server = BaseQueryServer(output=output)
        server.respond(['foo'])
        server.respond({'bar': 'baz'})
        self.assertEqual(output.getvalue(), '["foo"]\n{"bar": "baz"}\n')

    def test_log_oldstyle(self):
        output = StringIO()
        server = BaseQueryServer(version=(0, 9, 0), output=output)
        server.log(['foo', {'bar': 'baz'}, 42])
        self.assertEqual(
            output.getvalue(),
             '{"log": "[\\"foo\\", {\\"bar\\": \\"baz\\"}, 42]"}\n'
        )

    def test_log_none_message(self):
        output = StringIO()
        server = BaseQueryServer(version=(0, 9, 0), output=output)
        server.log(None)
        self.assertEqual(
            output.getvalue(),
             '{"log": "Error: attempting to log message of None"}\n'
        )

    def test_log_newstyle(self):
        output = StringIO()
        server = BaseQueryServer(version=(0, 11, 0), output=output)
        server.log(['foo', {'bar': 'baz'}, 42])
        self.assertEqual(
            output.getvalue(),
             '["log", "[\\"foo\\", {\\"bar\\": \\"baz\\"}, 42]"]\n'
        )

class SimpleQueryServerTestCase(unittest.TestCase):

    def setUp(self):
        self.output = StringIO()
        self.server = partial(SimpleQueryServer, output=self.output)

    def test_add_doc(self):
        server = self.server((0, 11, 0))
        self.assertTrue(server.add_ddoc({'_id': 'relax', 'at': 'couch'}))
        self.assertEqual(server.ddocs.cache['relax']['at'], 'couch')

    def test_add_fun(self):
        def foo():
            return 'bar'
        server = self.server()
        self.assertTrue(server.add_fun(foo))
        self.assertEqual(server.functions[0](), 'bar')

    def test_add_lib(self):
        server = SimpleQueryServer((1, 1, 0))
        self.assertTrue(server.add_lib({'foo': 'bar'}))
        self.assertEqual(server.view_lib, {'foo': 'bar'})

    def test_reset(self):
        server = self.server()
        server.query_config['foo'] = 'bar'
        self.assertTrue(server.reset())
        self.assertTrue('foo' not in server.query_config)

    def test_reset_set_new_config(self):
        server = self.server()
        self.assertTrue(server.reset({'foo': 'bar'}))
        self.assertTrue('foo' in server.query_config)

    def test_map_doc(self):
        def map_fun_1(doc):
            yield doc['_id'], 1
        def map_fun_2(doc):
            yield doc['_id'], 2
            yield doc['_id'], 3
        server = self.server()
        self.assertTrue(server.add_fun(map_fun_1))
        self.assertTrue(server.add_fun(map_fun_2))
        kvs = server.map_doc({'_id': 'foo'})
        self.assertEqual(
            kvs,
            [[['foo', 1]], [['foo', 2], ['foo', 3]]]
        )

    def test_reduce(self):
        def map_fun_1(doc):
            yield doc['_id'], 1
        def map_fun_2(doc):
            yield doc['_id'], 2
            yield doc['_id'], 3
        def red_fun_1(keys, values):
            return sum(values)
        def red_fun_2(keys, values):
            return min(values)
        server = self.server()
        self.assertTrue(server.add_fun(map_fun_1))
        self.assertTrue(server.add_fun(map_fun_2))
        kvs = server.map_doc({'_id': 'foo'})
        reduced = server.reduce([red_fun_1, red_fun_2], kvs[0])
        self.assertEqual(reduced, [True, [1, 1]])
        reduced = server.reduce([red_fun_1, red_fun_2], kvs[1])
        self.assertEqual(reduced, [True, [5, 2]])

    def test_rereduce(self):
        def red_fun(keys, values, rereduce):
            return sum(values)
        server = self.server()
        reduced = server.rereduce([red_fun], range(10))
        self.assertEqual(reduced, [True, [45]])

    def test_reduce_no_records(self):
        def red_fun(keys, values):
            return sum(values)
        server = self.server()
        reduced = server.reduce([red_fun], [])
        self.assertEqual(reduced, [True, [0]])

    def test_filter(self):
        def func(doc, req, userctx):
            return doc['q'] > 5
        server = self.server((0, 10, 0))
        result = server.filter(func, [{'q': 15}, {'q': 1}, {'q': 6}, {'q': 0}])
        self.assertEqual(result, [True, [True, False, True, False]])

    def test_ddoc_filter(self):
        def func(doc, req, userctx):
            return doc['q'] > 5
        server = self.server((0, 11, 0))
        server.add_ddoc(wrap_func_to_ddoc('foo', ['filters', 'gt_5'], func))
        result = server.ddoc_filter('foo', ['gt_5'], [{'q': 15}, {'q': -1}])
        self.assertEqual(result, [True, [True, False]])

    def test_ddoc_filter_view(self):
        def map_func(doc):
            if doc['q'] > 5:
                yield doc['q'], 1
        server = self.server((1, 1, 0))
        server.add_ddoc(wrap_func_to_ddoc('foo', ['views', 'gt5'], map_func))
        result = server.ddoc_filter_view('foo', ['gt5'], [[{'q': 7}, {'q': 1}]])
        self.assertEqual(result, [True, [True, False]])

    def test_validate_doc_update(self):
        def func(olddoc, newdoc, userctx):
            assert newdoc['q'] > 5
        server = self.server((0, 10, 0))
        result = server.validate_doc_update(func, {}, {'q': 42})
        self.assertEqual(result, 1)

    def test_ddoc_validate_doc_update(self):
        def func(olddoc, newdoc, userctx):
            assert newdoc['q'] > 5
        server = self.server((0, 11, 0))
        server.add_ddoc(wrap_func_to_ddoc('foo', ['validate_doc_update'], func))
        result = server.ddoc_validate_doc_update('foo', {}, {'q': 42})
        self.assertEqual(result, 1)

    def test_show_doc(self):
        def func(doc, req):
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
        server = self.server((0, 9, 0))
        doc = {'_id': 'couch'}
        req = {'headers': {'Accept': 'text/html,application/atom+xml; q=0.9'}}
        resp = server.show_doc(func, doc, req)
        self.assertEqual(
            resp,
            {
                'headers': {'Content-Type': 'text/html; charset=utf-8'},
                'body': '<html><body>couch</body></html>'
            }
        )
        
    def test_show(self):
        def func(doc, req):
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
        server = self.server((0, 10, 0))
        doc = {'_id': 'couch'}
        req = {'headers': {'Accept': 'text/html,application/atom+xml; q=0.9'}}
        token, resp = server.show(func, doc, req)
        self.assertEqual(token, 'resp')
        self.assertEqual(
            resp,
            {
                'headers': {'Content-Type': 'text/html; charset=utf-8'},
                'body': '<html><body>couch</body></html>'
            }
        )

    def test_ddoc_show(self):
        def func(doc, req):
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
        server = self.server((0, 11, 0))
        doc = {'_id': 'couch'}
        req = {'headers': {'Accept': 'text/html,application/atom+xml; q=0.9'}}
        server.add_ddoc(wrap_func_to_ddoc('foo', ['shows', 'provides'], func))
        token, resp = server.ddoc_show('foo', ['provides'], doc, req)
        self.assertEqual(token, 'resp')
        self.assertEqual(
            resp,
            {
                'headers': {'Content-Type': 'text/html; charset=utf-8'},
                'body': '<html><body>couch</body></html>'
            }
        )

    def test_list_old(self):
        def func(head, row, req, info):
            if head:
                return {'headers': {'Content-Type': 'text/plain'},
                        'code': 200,
                        'body': 'foo'}
            if row:
                return row['value']
            return 'tail'
        server = self.server((0, 9, 0))
        rows = [
            {'value': 'bar'},
            {'value': 'baz'},
            {'value': 'bam'},
        ]
        result = list(server.list_old(func, rows, {'foo': 'bar'}, {'q': 'ok'}))
        head, rows, tail = result[0], result[1:-1], result[-1]

        self.assertEqual(head, {'headers': {'Content-Type': 'text/plain'},
                                'code': 200, 'body': 'foo'})
        self.assertEqual(rows[0], {'body': 'bar'})
        self.assertEqual(rows[1], {'body': 'baz'})
        self.assertEqual(rows[2], {'body': 'bam'})
        self.assertEqual(tail, {'body': 'tail'})

    def test_list(self):
        def func(head, req):
            send('first chunk')
            send(req['q'])
            for row in get_row():
                send(row['key'])
            return 'early'
        server = self.server((0, 10, 0))
        rows = [
            {'key': 'foo'},
            {'key': 'bar'},
            {'key': 'baz'},
        ]
        result = server.list(func, rows, {'foo': 'bar'}, {'q': 'ok'})
        head, rows, tail = result[0], result[1:-1], result[-1]

        self.assertEqual(head, ['start', ['first chunk', 'ok'], {'headers': {}}])
        self.assertEqual(rows[0], ['chunks', ['foo']])
        self.assertEqual(rows[1], ['chunks', ['bar']])
        self.assertEqual(rows[2], ['chunks', ['baz']])
        self.assertEqual(tail, ['end', ['early']])

    def test_ddoc_list(self):
        def func(head, req):
            send('first chunk')
            send(req['q'])
            for row in get_row():
                send(row['key'])
            return 'early'
        server = self.server((0, 11, 0))
        rows = [
            {'key': 'foo'},
            {'key': 'bar'},
            {'key': 'baz'},
        ]
        server.add_ddoc(wrap_func_to_ddoc('foo', ['lists', 'fbb'], func))
        result = server.ddoc_list('foo', ['fbb'], rows, {'foo': 'bar'}, {'q': 'ok'})
        head, rows, tail = result[0], result[1:-1], result[-1]

        self.assertEqual(head, ['start', ['first chunk', 'ok'], {'headers': {}}])
        self.assertEqual(rows[0], ['chunks', ['foo']])
        self.assertEqual(rows[1], ['chunks', ['bar']])
        self.assertEqual(rows[2], ['chunks', ['baz']])
        self.assertEqual(tail, ['end', ['early']])

    def test_update(self):
        def func(doc, req):
            doc['world'] = 'hello'
            return [doc, 'hello, doc']

        server = self.server((0, 10, 0))
        result = server.update(func, {'_id': 'foo'})
        self.assertEqual(
            result,
            ['up', {'_id': 'foo', 'world': 'hello'}, {'body': 'hello, doc'}]
        )

    def test_ddoc_update(self):
        def func(doc, req):
            doc['world'] = 'hello'
            return [doc, 'hello, doc']

        server = self.server((0, 11, 0))
        server.add_ddoc(wrap_func_to_ddoc('foo', ['updates', 'hello'], func))
        result = server.ddoc_update('foo', ['hello'], {'_id': 'foo'})
        self.assertEqual(
            result,
            ['up', {'_id': 'foo', 'world': 'hello'}, {'body': 'hello, doc'}]
        )

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BaseQueryServerTestCase, 'test'))
    suite.addTest(unittest.makeSuite(SimpleQueryServerTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
