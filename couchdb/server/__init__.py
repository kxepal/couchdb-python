# -*- coding: utf-8 -*-
#
import logging
import sys
from couchdb import json
from couchdb.server import compiler, ddoc, exceptions, filters, render, \
                           state, stream, validate, views
from couchdb.server.helpers import partial, maybe_extract_source

__all__ = ['BaseQueryServer', 'SimpleQueryServer']

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
            'row_line': {}
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

        .. versionchanged:: 0.11.0
            Log message format has changed from ``{"log": message}`` to
            ``["log", message]``
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
        options.update(self.config)
        return compiler.compile_func(funsrc, ddoc, context, **options)

    def process_request(self, message):
        """Process single request message.

        :param message: Message list of two elements: command name and list
                        command arguments, which would be passed to command
                        handler function.
        :type message: list

        :returns: Command handler result.
        
        :raises:
            - :exc:`~couchdb.server.exceptions.FatalError` if no handlers was
              registered for processed command.
        """
        try:
            return self._process_request(message)
        except Exception:
            self.handle_exception(*sys.exc_info())
    
    def _process_request(self, message):
        cmd, args = message.pop(0), message
        log.debug('Process command `%s`', cmd)
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
        """Runs ``add_lib`` command.

        :param mod: Module in CommonJS style
        :type mod: dict

        :return: True

        .. versionadded:: 1.1.0
        """
        return self._process_request(['add_lib', mod])

    def add_fun(self, fun):
        """Runs ``add_fun`` command.

        :param fun: Function object or source string.
        :type fun: function or str

        :return: True

        .. versionadded:: 0.8.0
        """
        funsrc = maybe_extract_source(fun)
        return self._process_request(['add_fun', funsrc])

    def add_ddoc(self, ddoc):
        """Runs ``ddoc`` command to teach query server new design document.

        :param ddoc: Design document. Should contains ``_id`` key.
        :type ddoc: dict

        :return: True

        .. versionadded:: 0.11.0
        """
        return self._process_request(['ddoc', 'new', ddoc['_id'], ddoc])

    def map_doc(self, doc):
        """Runs ``map_doc`` command to apply known map functions to doc.
        Requires at least one stored function via :meth:`add_fun`.

        :param doc: Document object.
        :type doc: dict

        :return: List of key-values pairs per applied function.

        .. versionadded:: 0.8.0
        """
        return self._process_request(['map_doc', doc])

    def reduce(self, funs, keysvalues):
        """Runs ``reduce`` command.

        :param funs: List of function objects or source strings.
        :type funs: list

        :param keysvalues: List of 2-element tuples with key and value.
        :type: list

        :return: Two-element list with True value and list of values per
                 reduce function.

        .. versionadded:: 0.8.0
        """
        funsrcs = [maybe_extract_source(fun) for fun in funs]
        return self._process_request(['reduce', funsrcs, keysvalues])

    def rereduce(self, funs, values):
        """Runs ``rereduce`` command.

        :param funs: List of function objects or source strings.
        :type funs: list

        :param values: List of 2-element tuples with key and value.
        :type: list

        :return: Two-element list with True value and list of values per
                 reduce function.

        .. versionadded:: 0.8.0
        """
        funsrcs = [maybe_extract_source(fun) for fun in funs]
        return self._process_request(['rereduce', funsrcs, values])

    def reset(self, config=None):
        """Runs ``reset`` command.

        :param config: New query server config options.
        :type config: dict

        :return: True

        .. versionadded:: 0.8.0
        """
        if config:
            return self._process_request(['reset', config])
        else:
            return self._process_request(['reset'])

    def show_doc(self, fun, doc=None, req=None):
        """Runs ``show_doc`` command.

        :param fun: Function object or source string.
        :type fun: function or str

        :param doc: Document object.
        :type doc: dict

        :param req: Request object.
        :type req: dict

        :return: Two-element list with `resp` token and Response object.

        .. versionadded:: 0.9.0
        .. deprecated:: 0.10.0 Use :meth:`show` instead.
        """
        funsrc = maybe_extract_source(fun)
        return self._process_request(['show_doc', funsrc, doc or {}, req or {}])

    def list_old(self, fun, rows, head=None, req=None):
        """Runs ``list_begin``, ``list_row`` and ``list_tail`` commands.
        Implicitly resets and adds passed function to query server state.

        :param fun: Function object or source string.
        :type fun: function or str

        :param rows: View result rows as list of dicts with `id`, `key`
                     and `value` keys.
        :type rows: list

        :param req: Request object.
        :type req: dict

        :yield: Two-element lists with token and response object.
                 First element is for ``list_begin`` command with `start` token,
                 last one is for ``list_tail`` command with `end` token
                 and others for ``list_row`` commands with `chunk` token.

        .. versionadded:: 0.9.0
        .. deprecated:: 0.10.0 Use :meth:`list` instead.
        """
        self.reset()
        self.add_fun(fun)
        head, req = head or {}, req or {}
        yield self._process_request(['list_begin', head, req])
        for row in rows:
            yield self._process_request(['list_row', row, req])
        yield self._process_request(['list_tail', req])

    def show(self, fun, doc=None, req=None):
        """Runs ``show`` command.

        :param fun: Function object or source string.
        :type fun: function or str

        :param doc: Document object.
        :type doc: dict

        :param req: Request object.
        :type req: dict

        :return: Two-element list with `resp` token and Response object.

        .. versionadded:: 0.10.0
        .. deprecated:: 0.11.0 Use :meth:`ddoc_show` instead.
        """
        funsrc = maybe_extract_source(fun)
        return self._process_request(['show', funsrc, doc or {}, req or {}])

    def list(self, fun, rows, head=None, req=None):
        """Runs ``list`` command.
        Implicitly resets and adds passed function to query server state.

        :param fun: Function object or source string.
        :type fun: function or str

        :param rows: View result rows as list of dicts with `id`, `key`
                     and `value` keys.
        :type rows: list

        :param req: Request object.
        :type req: dict

        :return: Two-element lists with token and data chunks.
                 First element is for ``list_begin`` command with `start` token,
                 last one is for ``list_tail`` command with `end` token
                 and others for ``list_row`` commands with `chunk` token.

        .. versionadded:: 0.10.0
        .. deprecated:: 0.11.0 Use :meth:`ddoc_list` instead.
        """
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

    def update(self, fun, doc=None, req=None):
        """Runs ``update`` command.

        :param fun: Function object or source string.
        :type fun: function or str

        :param doc: Document object.
        :type doc: dict

        :param req: Request object.
        :type req: dict

        :return: Three-element list with ``up`` token, new document object
                 and response object.

        .. versionadded:: 0.10.0
        .. deprecated:: 0.11.0 Use :meth:`ddoc_update` instead.
        """
        funstr = maybe_extract_source(fun)
        return self._process_request(['update', funstr, doc or {}, req or {}])

    def filter(self, fun, docs, req=None):
        """Runs ``filter`` command.
        Implicitly resets and adds passed function to query server state.

        :param fun: Function object or source string.
        :type fun: function or str

        :param docs: List of document objects.
        :type docs: list

        :param req: Request object.
        :type req: dict

        :return: List of chunks.

        .. versionadded:: 0.10.0
        .. deprecated:: 0.11.0 Use :meth:`ddoc_filter` instead.
        """
        self.reset()
        self.add_fun(fun)
        return self._process_request(['filter', docs, req or {}])

    def validate_doc_update(self, fun, olddoc=None, newdoc=None, userctx=None):
        """Runs ``validate`` command.

        :param fun: Function object or source string.
        :type fun: function or str

        :param olddoc: Document object.
        :type olddoc: dict

        :param newdoc: Document object.
        :type newdoc: dict

        :param userctx: User context object.
        :type userctx: dict

        :return: 1

        .. versionadded:: 0.10.0
        .. deprecated:: 0.11.0 Use :meth:`ddoc_validate_doc_update` instead.
        """
        funsrc = maybe_extract_source(fun)
        args = [olddoc or {}, newdoc or {}, userctx or {}]
        return self._process_request(['validate', funsrc] + args)

    def ddoc_cmd(self, ddoc_id, cmd, func_path, func_args):
        """Runs ``ddoc`` command.
        Requires teached ddoc by :meth:`add_ddoc`.

        :param ddoc_id: DDoc id.
        :type ddoc_id: str

        :param cmd: Command name.
        :type cmd: str

        :param func_path: List of keys which holds filter function within ddoc.
        :type func_path: list

        :param func_args: List of design function arguments.
        :type func_args: list

        :return: Returned value depended from executed command.

        .. versionadded:: 0.11.0
        """
        if not func_path or func_path[0] != cmd:
            func_path.insert(0, cmd)
        return self._process_request(['ddoc',  ddoc_id, func_path, func_args])

    def ddoc_show(self, ddoc_id, func_path, doc=None, req=None):
        """Runs ``ddoc`` ``shows`` command.
        Requires teached ddoc by :meth:`add_ddoc`.

        :param ddoc_id: DDoc id.
        :type ddoc_id: str

        :param func_path: List of keys which holds filter function within ddoc.
        :type func_path: list

        :param doc: Document object.
        :type doc: dict

        :param req: Request object.
        :type req: dict

        :return: Two-element list with `resp` token and Response object.

        .. versionadded:: 0.11.0
        """
        args =  [doc or {}, req or {}]
        return self.ddoc_cmd(ddoc_id, 'shows', func_path, args)

    def ddoc_list(self, ddoc_id, func_path, rows, head=None, req=None):
        """Runs ``ddoc`` ``lists`` command.
        Requires teached ddoc by :meth:`add_ddoc`.

        :param ddoc_id: DDoc id.
        :type ddoc_id: str

        :param func_path: List of keys which holds filter function within ddoc.
        :type func_path: list

        :param rows: View result rows as list of dicts with `id`, `key`
                     and `value` keys.
        :type rows: list

        :param req: Request object.
        :type req: dict

        :return: Two-element lists with token and data chunks.
                 First element is for ``list_begin`` command with `start` token,
                 last one is for ``list_tail`` command with `end` token
                 and others for ``list_row`` commands with `chunk` token.

        .. versionadded:: 0.11.0
        """
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
        """Runs ``ddoc`` ``updates`` command.
        Requires teached ddoc by :meth:`add_ddoc`.

        :param ddoc_id: DDoc id.
        :type ddoc_id: str

        :param func_path: List of keys which holds filter function within ddoc.
        :type func_path: list

        :param doc: Document object.
        :type doc: dict

        :param req: Request object.
        :type req: dict

        :return: Three-element list with ``up`` token, new document object
                 and response object.

        .. versionadded:: 0.11.0
        """
        args = [doc or {}, req or {}]
        return self.ddoc_cmd(ddoc_id, 'updates', func_path, args)

    def ddoc_filter(self, ddoc_id, func_path, docs, req=None, userctx=None):
        """Runs ``ddoc`` ``filters`` command.
        Requires teached ddoc by :meth:`add_ddoc`.

        :param ddoc_id: DDoc id.
        :type ddoc_id: str

        :param func_path: List of keys which holds filter function within ddoc.
        :type func_path: list

        :param docs: List of document objects.
        :type docs: list

        :param req: Request object.
        :type req: dict

        :param userctx: User context object.
        :type userctx: dict

        :return: Two-element list with True and boolean value for each document
                 which sign was document passed filter or not.

        .. versionadded:: 0.11.0
        """
        args = [docs, req or {}, userctx or {}]
        return self.ddoc_cmd(ddoc_id, 'filters', func_path, args)

    def ddoc_filter_view(self, ddoc_id, func_path, docs):
        """Runs ``ddoc`` ``views`` command.
        Requires teached ddoc by :meth:`add_ddoc`.

        :param ddoc_id: DDoc id.
        :type ddoc_id: str

        :param func_path: List of keys which holds filter function within ddoc.
        :type func_path: list

        :param docs: List of document objects.
        :type docs: list

        :return: Two-element list with True and boolean value for each document
                 which sign was document passed filter or not.
        """
        return self.ddoc_cmd(ddoc_id, 'views', func_path, docs)

    def ddoc_validate_doc_update(self, ddoc_id, olddoc=None,
                                 newdoc=None, userctx=None, secobj=None):
        """Runs ``ddoc`` ``validate_doc_update`` command.
        Requires teached ddoc by :meth:`add_ddoc`.

        :param ddoc_id: DDoc id.
        :type ddoc_id: str

        :param olddoc: Document object.
        :type olddoc: dict

        :param newdoc: Document object.
        :type newdoc: dict

        :param userctx: User context object.
        :type userctx: dict

        :return: 1

        .. versionadded:: 0.11.0
        """
        args = [olddoc or {}, newdoc or {}, userctx or {}, secobj or {}]
        return self.ddoc_cmd(ddoc_id, 'validate_doc_update', [], args)

    @property
    def ddocs(self):
        """Returns dict with registered ddocs"""
        return self.commands['ddoc']

    @property
    def functions(self):
        """Returns dict with registered ddocs"""
        return self.state['functions']

    @property
    def query_config(self):
        """Returns query server config which :meth:`reset` operates with"""
        return self.state['query_config']

    @property
    def view_lib(self):
        """Returns stored view lib which could be added by :meth:`add_lib`"""
        return self.state['view_lib']
