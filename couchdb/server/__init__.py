# -*- coding: utf-8 -*-
#
import logging
import sys
from couchdb import json
from couchdb.server import compiler, exceptions, stream

try:
    from functools import partial
except ImportError:
    def partial(func, *args, **keywords):
        def newfunc(*fargs, **fkeywords):
            newkeywords = keywords.copy()
            newkeywords.update(fkeywords)
            return func(*(args + fargs), **newkeywords)
        newfunc.func = func
        newfunc.args = args
        newfunc.keywords = keywords
        return newfunc


class NullHandler(logging.Handler):
    def emit(self, *args, **kwargs):
        pass


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(NullHandler())


class BaseQueryServer(object):
    """Implements Python CouchDB query server.

    :param version: CouchDB server version as three int elements tuple.
                    By default tries to work against highest implemented one.
    :type version: tuple

    :param options: Custom keyword arguments.
    """
    def __init__(self, version=None, **options):
        """Initialize query server instance."""

        input = options.pop('input', sys.stdin)
        output = options.pop('output', sys.stdout)
        self._receive = partial(stream.receive, input=input)
        self._respond = partial(stream.respond, output=output)
        
        self._version = version or (999, 999, 999)

        self._commands = {}
        self._commands_ddoc = {}
        self._ddoc_cache = {}

        self._config = {}
        self._state = {
            'view_lib': None,
            'line_length': 0,
            'query_config': {},
            'functions': [],
            'functions_src': [],
        }

        for key, value in options.items():
            self.handle_config(key, value)

    def config_log_level(self, value):
        """Sets overall logging level.

        :param value: Valid logging level name.
        :type value: str
        """
        log.setLevel(getattr(logging, value.upper(), 'INFO'))

    def config_log_file(self, value):
        """Sets logging file handler. Not used by default.

        :param value: Log file path.
        :type value: str
        """
        handler = logging.FileHandler(value)
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
        ))
        log.addHandler(handler)

    @property
    def config(self):
        """Proxy to query server configuration dictionary. Contains global
        config options."""
        return self._config

    @property
    def state(self):
        """Query server state dictionary. Also contains ``query_config``
        dictionary which specified by CouchDB server configuration."""
        return self._state

    @property
    def commands(self):
        """Dictionary of supported command names (keys) and their handlers
        (values)."""
        return self._commands

    @property
    def version(self):
        """Returns CouchDB version against QueryServer instance is suit."""
        return self._version

    def handle_exception(self, exc_type, exc_value, exc_traceback, default=None):
        """Exception dispatcher.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.

        :param default: Custom default handler.
        :type default: callable
        """
        handler = {
            exceptions.Forbidden: self.handle_forbidden_error,
            exceptions.Error: self.handle_qs_error,
            exceptions.FatalError: self.handle_fatal_error,
        }.get(exc_type, default or self.handle_python_exception)
        return handler(exc_type, exc_value, exc_traceback)

    def handle_config(self, key, value):
        """Handles config options.

        :param key: Config option name.
        :type key: str

        :param value:
        """
        hname = 'config_%s' % key
        if hasattr(self, hname):
            getattr(self, hname)(value)
        else:
            self.config[key] = value

    def handle_fatal_error(self, exc_type, exc_value, exc_traceback):
        """Handler for :exc:`~couchdb.server.exceptions.FatalError` exceptions.

        Terminates query server.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        """
        log.exception('FatalError `%s` occurred: %s', *exc_value.args)
        if self.version < (0, 11, 0):
            id, reason = exc_value.args
            retval = {'error': id, 'reason': reason}
        else:
            retval = ['error'] + list(exc_value.args)
        self.respond(retval)
        log.critical('That was a critical error, exiting')
        raise

    def handle_qs_error(self, exc_type, exc_value, exc_traceback):
        """Handler for :exc:`~couchdb.server.exceptions.Error` exceptions.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        """
        log.exception('Error `%s` occurred: %s', *exc_value.args)
        if self.version < (0, 11, 0):
            id, reason = exc_value.args
            retval = {'error': id, 'reason': reason}
        else:
            retval = ['error'] + list(exc_value.args)
        self.respond(retval)

    def handle_forbidden_error(self, exc_type, exc_value, exc_traceback):
        """Handler for :exc:`~couchdb.server.exceptions.Forbidden` exceptions.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        """
        reason = exc_value.args[0]
        log.warn('ForbiddenError occurred: %s', reason)
        self.respond({'forbidden': reason})

    def handle_python_exception(self, exc_type, exc_value, exc_traceback):
        """Handler for any Python occurred exception.

        Terminates query server.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        """
        err_name = exc_type.__name__
        err_msg = str(exc_value)
        log.exception('%s: %s', err_name, err_msg)
        if self.version < (0, 11, 0):
            retval = {'error': err_name, 'reason': err_msg}
        else:
            retval = ['error', err_name, err_msg]
        self.respond(retval)
        log.critical('That was a critical error, exiting')
        raise

    def serve_forever(self):
        """Query server main loop. Runs forever or till input stream is opened.

        :returns:
            - 0 (`int`): If :exc:`KeyboardInterrupt` exception occurred or
              server has terminated gracefully.
            - 1 (`int`): If server has terminated by
              :exc:`~couchdb.server.exceptions.FatalError` or by another one.
        """
        try:
            for message in self.receive():
                self.respond(self.process_request(message))
        except KeyboardInterrupt:
            return 0
        except exceptions.FatalError:
            return 1
        except Exception:
            return 1
        else:
            return 0

    def receive(self):
        """Returns iterable object over lines of input data."""
        return self._receive()

    def respond(self, data):
        """Sends data to output stream.

        :param data: JSON encodable object.
        """
        return self._respond(data)

    def log(self, message):
        """Log message to CouchDB output stream.

        Output format:
            till 0.11.0 version: {"log": message}
            since 0.11.0 version: ["log", message]
        """
        if self.version < (0, 11, 0):
            if message is None:
                message = 'Error: attempting to log message of None'
            if not isinstance(message, basestring):
                message = json.encode(message)
            res = {'log': message}
        else:
            if not isinstance(message, basestring):
                message = json.encode(message)
            res = ['log', message]
        self.respond(res)

    def compile(self, funsrc, ddoc=None, context=None, **options):
        """Compiles function with special server context.

        :param funsrc: Function source code.
        :type funsrc: str
        
        :param ddoc: Design document object.
        :type ddoc: dict

        :param context: Custom context for compiled function.
        :type context: dict

        :param options: Compiler config options.
        """
        if context is None:
            context = {}
        context.setdefault('log', self.log)
        return compiler.compile_func(funsrc, ddoc, context, **options)

    def process_request(self, message):
        """Process single request message.

        :param message: Message list of two elements: command name and list
                        command arguments, which would be passed to command
                        handler function.
        :type message: list

        :returns: Command handler result.
        
        :raises:
            - :exc:`~couchdb.server.exception.FatalError` if no handlers was
              registered for processed command.
        """
        try:
            return self._process_request(message)
        except Exception:
            self.handle_exception(*sys.exc_info())

    def _process_request(self, message):
        cmd, args = message.pop(0), message
        log.debug('Processing command `%s`', cmd)
        if cmd not in self.commands:
            raise exceptions.FatalError('unknown_command',
                                        'unknown command %s' % cmd)
        return self.commands[cmd](self, *args)

    def is_reduce_limited(self):
        """Checks if output of reduce function is limited."""
        return self.state['query_config'].get('reduce_limit', False)
