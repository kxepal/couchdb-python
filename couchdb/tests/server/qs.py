# -*- coding: utf-8 -*-
#
import unittest
from cStringIO import StringIO
from couchdb.server import BaseQueryServer
from couchdb.server import exceptions

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


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BaseQueryServerTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
