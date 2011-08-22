# -*- coding: utf-8 -*-
#
import logging
import sys
from couchdb import json
from couchdb.server import compiler, ddoc, exceptions, filters, render, \
                           state, stream, validate, views
from couchdb.server.helpers import partial, maybe_extract_source


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


class SimpleQueryServer(BaseQueryServer):
    """Implements Python query server with high level API."""

    def __init__(self, *args, **kwargs):
        super(SimpleQueryServer, self).__init__(*args, **kwargs)

        self.commands['reset'] = state.reset
        self.commands['add_fun'] = state.add_fun

        self.commands['map_doc'] = views.map_doc
        self.commands['reduce'] = views.reduce
        self.commands['rereduce'] = views.rereduce

        if (0, 9, 0) <= self.version < (0, 10, 0):
            self.commands['show_doc'] = render.show_doc
            self.commands['list_begin'] = render.list_begin
            self.commands['list_row'] = render.list_row
            self.commands['list_tail'] = render.list_tail
            self.commands['validate'] = validate.validate

        elif (0, 10, 0) <= self.version < (0, 11, 0):
            self.commands['show'] = render.show
            self.commands['list'] = render.list
            self.commands['filter'] = filters.filter
            self.commands['update'] = render.update
            self.commands['validate'] = validate.validate

        elif self.version >= (0, 11, 0):
            ddoc_commands = {}
            ddoc_commands['shows'] = render.ddoc_show
            ddoc_commands['lists'] = render.ddoc_list
            ddoc_commands['filters'] = filters.ddoc_filter
            ddoc_commands['updates'] = render.ddoc_update
            ddoc_commands['validate_doc_update'] = validate.ddoc_validate

        if self.version >= (1, 1, 0):
            self.commands['add_lib'] = state.add_lib
            ddoc_commands['views'] = filters.ddoc_views

        if self.version >= (0, 11, 0):
            self.commands['ddoc'] = ddoc.DDoc(ddoc_commands)

    def add_lib(self, mod):
        return self._process_request(['add_lib', mod])

    def add_fun(self, fun):
        funsrc = maybe_extract_source(fun)
        return self._process_request(['add_fun', funsrc])

    def add_ddoc(self, ddoc):
        return self._process_request(['ddoc', 'new', ddoc['_id'], ddoc])

    def map_doc(self, doc):
        return self._process_request(['map_doc', doc])

    def reduce(self, funs, keysvalues):
        funsrcs = map(maybe_extract_source, funs)
        return self._process_request(['reduce', funsrcs, keysvalues])

    def rereduce(self, funs, values):
        funsrcs = map(maybe_extract_source, funs)
        return self._process_request(['rereduce', funsrcs, values])

    def reset(self, config=None):
        if config:
            return self._process_request(['reset', config])
        else:
            return self._process_request(['reset'])

    def show_doc(self, fun, doc=None, req=None):
        funsrc = maybe_extract_source(fun)
        return self._process_request(['show_doc', funsrc, doc or {}, req or {}])

    def list_old(self, fun, rows, head=None, req=None):
        self.add_fun(fun)
        head, req = head or {}, req or {}
        yield self._process_request(['list_begin', head, req])
        for row in rows:
            yield self._process_request(['list_row', row, req])
        yield self._process_request(['list_tail', req])

    def show(self, fun, doc=None, req=None):
        funsrc = maybe_extract_source(fun)
        return self._process_request(['show', funsrc, doc or {}, req or {}])

    def list(self, fun, rows, head=None, req=None):
        self.reset()
        self.add_fun(fun)

        result, input_rows = [], []
        for row in rows:
            input_rows.append(['list_row', row])
        input_rows.append(['list_end'])
        input_rows = iter(input_rows)

        _input, _output = self._receive, self._respond
        self._receive, self._respond = (lambda: input_rows), result.append

        self._process_request(['list', head or {}, req or {}])

        self._receive, self._respond = _input, _output
        return result

    def update(self, func, doc=None, req=None):
        funstr = maybe_extract_source(func)
        return self._process_request(['update', funstr, doc or {}, req or {}])

    def filter(self, func, docs, req=None):
        self.reset()
        self.add_fun(func)
        return self._process_request(['filter', docs, req or {}])

    def validate_doc_update(self, func, olddoc=None, newdoc=None, userctx=None):
        funsrc = maybe_extract_source(func)
        args = [olddoc or {}, newdoc or {}, userctx or {}]
        return self._process_request(['validate', funsrc] + args)

    def ddoc_cmd(self, ddoc_id, cmd, func_path, func_args):
        assert isinstance(func_path, list)
        assert isinstance(func_args, list)
        if not func_path or func_path[0] != cmd:
            func_path.insert(0, cmd)
        return self._process_request(['ddoc',  ddoc_id, func_path, func_args])

    def ddoc_show(self, ddoc_id, func_path, doc=None, req=None):
        args =  [doc or {}, req or {}]
        return self.ddoc_cmd(ddoc_id, 'shows', func_path, args)

    def ddoc_list(self, ddoc_id, func_path, rows, head=None, req=None):
        args = [head or {}, req or {}]

        result, input_rows = [], []
        for row in rows:
            input_rows.append(['list_row', row])
        input_rows.append(['list_end'])
        input_rows = iter(input_rows)

        _input, _output = self._receive, self._respond
        self._receive, self._respond = (lambda: input_rows), result.append

        self.ddoc_cmd(ddoc_id, 'lists', func_path, args)

        self._receive, self._respond = _input, _output
        return result

    def ddoc_update(self, ddoc_id, func_path, doc=None, req=None):
        args = [doc or {}, req or {}]
        return self.ddoc_cmd(ddoc_id, 'updates', func_path, args)

    def ddoc_filter(self, ddoc_id, func_path, docs, req=None, userctx=None):
        args = [docs, req or {}, userctx or {}]
        return self.ddoc_cmd(ddoc_id, 'filters', func_path, args)

    def ddoc_filter_view(self, ddoc_id, func_path, docs):
        assert isinstance(docs, list)
        return self.ddoc_cmd(ddoc_id, 'views', func_path, docs)

    def ddoc_validate_doc_update(self, ddoc_id, olddoc=None,
                                 newdoc=None, userctx=None, secobj=None):
        args = [olddoc or {}, newdoc or {}, userctx or {}, secobj or {}]
        return self.ddoc_cmd(ddoc_id, 'validate_doc_update', [], args)

    @property
    def ddocs(self):
        return self.commands['ddoc']

    @property
    def functions(self):
        return self.state['functions']

    @property
    def query_config(self):
        return self.state['query_config']

    @property
    def view_lib(self):
        return self.state['view_lib']
