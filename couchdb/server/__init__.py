# -*- coding: utf-8 -*-
#
import logging
import sys
from couchdb import json
from couchdb.server import compiler, ddoc, exceptions, filters, mime, render, \
                           state, stream, validate, views


class NullHandler(logging.Handler):
    def emit(self, *args, **kwargs):
        pass


class ViewServerHandler(logging.Handler):

    def emit(self, record):
        '''Logs message to CouchDB output stream.

        Output format:
            till 0.11.0 version: {"log": message}
            since 0.11.0 version: ["log", message]
        '''
        message = self.format(record)
        if state.version < (0, 11, 0):
            if message is None:
                message = 'Error: attemting to log message of None'
            if not isinstance(message, basestring):
                message = json.encode(message)
            res = {'log': message}
        else:
            if not isinstance(message, basestring):
                message = json.encode(message)
            res = ['log', message]
        stream.respond(res)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(NullHandler())
context_log = logging.getLogger(__name__ + '.design_function')
context_log.setLevel(logging.INFO)
context_log.addHandler(ViewServerHandler())


class QueryServer(object):
    '''Implements Python CouchDB query server.'''
    def __init__(self, version=(999, 999, 999), **options):
        '''Initialize query server instance.

        :param version: CouchDB server version as three int elements tuple.
                        By default tries to work againt highest implemented one.
        :type version: tuple
        '''
        state.reset()
        state.version = version

        self.commands = {}
        self.error_handlers = {
            exceptions.Error: self.handle_qs_error,
            exceptions.FatalError: self.handle_fatal_error,
            exceptions.Forbidden: self.handle_forbidden_error,
            'default': self.handle_exception
        }
        self.options = {
            'allow_get_update': self.config_allow_get_update,
            'enable_eggs': self.config_enable_eggs,
            'egg_cache': self.config_egg_cache,
            'log_level': self.config_log_level,
            'log_file': self.config_log_file,
        }

        for key in list(options):
            if key in self.options:
                self.options[key](options.pop(key))

        if options:
            raise ValueError('Unknown query server config options %r' % options)

    def config_allow_get_update(self, value):
        state.allow_get_update = True

    def config_enable_eggs(self, value):
        state.enable_eggs = True

    def config_egg_cache(self, value):
        state.egg_cache = value

    def config_log_level(self, value):
        log.setLevel(getattr(logging, value.upper(), 'INFO'))

    def config_log_file(self, value):
        handler = logging.FileHandler(value)
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
        ))
        log.addHandler(handler)

    def add_command(self, cmd, handler):
        '''Registers new query server command and binds handler for it.

        :param cmd: Command name.
        :param handler: Callable object which would called on passed command.
        :type cmd: basestring
        :type handler: callable
        '''
        self.commands[cmd] = handler

    def add_command_ddoc(self, cmd, handler):
        '''Registers new design document command (actualy this is design
        document top level field) and binds handler for it.

        :param cmd: Command name.
        :param handler: Callable object which would called on passed command.
        :type cmd: basestring
        :type handler: callable
        '''
        ddoc.commands[cmd] = handler

    def add_context_object(self, name, obj):
        '''Adds new object to design function execution context, which would
        be accessed by specified name.

        :param name: Object access name.
        :param obj: Any object, function, variable.
        :type name: basestring
        :type obj: any
        '''
        compiler.context[name] = obj

    @property
    def version(self):
        '''Returns CouchDB version against this QueryServer instance is runned.'''
        return state.version

    def handle_fatal_error(self, exc_type, exc_value, exc_traceback):
        '''Handler for :exc:`~couchdb.server.exceptions.FatalError` exceptions.

        Terminates query server.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        :type exc_type: class object.
        :type exc_value: class instance.
        :type exc_traceback: traceback.

        :return: 1
        :rtype: int
        '''
        log.exception('FatalError `%s` occured: %s', *exc_value.args)
        if state.version < (0, 11, 0):
            id, reason = exc_value.args
            retval = {'error': id, 'reason': reason}
        else:
            retval = ['error'] + list(exc_value.args)
        stream.respond(retval)
        log.critical('That was a critical error, exiting')
        return 1

    def handle_qs_error(self, exc_type, exc_value, exc_traceback):
        '''Handler for :exc:`~couchdb.server.exceptions.Error` exceptions.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        :type exc_type: class object.
        :type exc_value: class instance.
        :type exc_traceback: traceback.
        '''
        log.exception('Error `%s` occured: %s', *exc_value.args)
        if state.version < (0, 11, 0):
            id, reason = exc_value.args
            retval = {'error': id, 'reason': reason}
        else:
            retval = ['error'] + list(exc_value.args)
        stream.respond(retval)

    def handle_forbidden_error(self, exc_type, exc_value, exc_traceback):
        '''Handler for :exc:`~couchdb.server.exceptions.Forbidden` exceptions.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        :type exc_type: class object.
        :type exc_value: class instance.
        :type exc_traceback: traceback.
        '''
        reason = exc_value.args[0]
        log.warn('ForbiddenError occured: %s', reason)
        retval = {'forbidden': reason}
        stream.respond(retval)

    def handle_exception(self, type, value, traceback):
        '''Handler for any Python occured exception.

        Terminates query server.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        :type exc_type: class object.
        :type exc_value: class instance.
        :type exc_traceback: traceback.

        :return: 1
        :rtype: int
        '''
        err_name = type.__name__
        err_msg = str(value)
        log.exception('%s: %s', err_name, err_msg)
        if state.version < (0, 11, 0):
            retval = {'error': err_name, 'reason': err_msg}
        else:
            retval = ['error', err_name, err_msg]
        stream.respond(retval)
        log.critical('That was a critical error, exiting')
        return 1

    def error_handler(self, type, value, traceback, default=None):
        '''Exception handling dispatcher for query server main loop.

        :param exc_type: Exception type.
        :param exc_value: Exception instance.
        :param exc_traceback: Actual exception traceback.
        :param default: Custom default handler.
        :type exc_type: class object.
        :type exc_value: class instance.
        :type exc_traceback: traceback.
        :type default: callable
        '''
        if default is None:
            default = self.error_handlers['default']
        return self.error_handlers.get(type, default)(type, value, traceback)

    def run(self, input=None, output=None):
        '''Query server main loop.

        :param input: The readable file-like object to read input from.
                      Default ``sys.stdin``.
        :param output: The writable file-like object to write output to.
                       Default ``sys.stdout``.
        '''
        stream.input = input or sys.stdin
        stream.output = output or sys.stdout
        try:
            for message in stream.receive():
                try:
                    cmd, args = message.pop(0), message
                    log.debug('Processing command `%s`', cmd)
                    if cmd not in self.commands:
                        raise exceptions.FatalError('unknown_command',
                                                    'unknown command %s' % cmd)
                    retval = self.commands[cmd](*args)
                except Exception, err:
                    retval = self.error_handler(*sys.exc_info())
                    if retval is not None:
                        return retval
                else:
                    stream.respond(retval)
        except KeyboardInterrupt:
            return 0
        except Exception, err:
            self.error_handler(*sys.exc_info())
            log.exception('Unexpected error occured: %s', err)
            log.critical('That was a critical error, exiting')
            return 1


def construct_server(version=None, **config):
    '''Constructs default Python query server with support of all features
    for specified CouchDB version.

    Supports CouchDB features from 0.8.0 till 1.1.0 version.

    :param version: CouchDB server version as three int elements tuple.
                    By default tries to work againt highest implemented one.
    :type version: tuple
    '''
    qs = QueryServer(version, **config)

    qs.add_command('reset', state.reset)
    qs.add_command('add_fun', state.add_fun)

    qs.add_command('map_doc', views.map_doc)
    qs.add_command('reduce', views.reduce)
    qs.add_command('rereduce', views.rereduce)

    qs.add_context_object('log', context_log.info)
    qs.add_context_object('json', json)
    qs.add_context_object('FatalError', exceptions.FatalError)
    qs.add_context_object('Error', exceptions.Error)
    qs.add_context_object('Forbidden', exceptions.Forbidden)

    if qs.version >= (0, 9, 0):
        qs.add_context_object('provides', mime.provides)
        qs.add_context_object('register_type', mime.register_type)

    if qs.version >= (0, 10, 0):
        qs.add_context_object('start', render.start)
        qs.add_context_object('get_row', render.get_row)
        qs.add_context_object('send', render.send)

    if (0, 9, 0) <= qs.version < (0, 10, 0):
        qs.add_command('show_doc', render.show_doc)
        qs.add_command('list_begin', render.list_begin)
        qs.add_command('list_row', render.list_row)
        qs.add_command('list_tail', render.list_tail)
        qs.add_command('validate', validate.validate)

        qs.add_context_object('response_with', render.response_with)

    elif (0, 10, 0) <= qs.version < (0, 11, 0):
        qs.add_command('show', render.show)
        qs.add_command('list', render.list)
        qs.add_command('filter', filters.filter)
        qs.add_command('update', render.update)
        qs.add_command('validate', validate.validate)

    elif qs.version >= (0, 11, 0):
        qs.add_command('ddoc', ddoc.ddoc)
        qs.add_command_ddoc('shows', render.ddoc_show)
        qs.add_command_ddoc('lists', render.ddoc_list)
        qs.add_command_ddoc('filters', filters.ddoc_filter)
        qs.add_command_ddoc('updates', render.ddoc_update)
        qs.add_command_ddoc('validate_doc_update', validate.ddoc_validate)

    if qs.version >= (1, 1, 0):
        qs.add_command('add_lib', state.add_lib)

    if qs.version > (1, 1, 0):
        qs.add_command_ddoc('views', filters.ddoc_views)

    return qs
