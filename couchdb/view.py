#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2008 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Implementation of a view server for functions written in Python."""
import traceback

from codecs import BOM_UTF8
import logging
import os
import sys
from types import FunctionType

from couchdb import json
from couchdb.util import CouchDBVersion

__all__ = ['main', 'run']
__docformat__ = 'restructuredtext en'

log = logging.getLogger('couchdb.view')

class ViewServerException(Exception):

    @CouchDBVersion.minimal(0, 9, 0)
    def encode(self):
        err_type, id, reason = self.args
        return {err_type: id, 'reason': reason}

    @CouchDBVersion.minimal(0, 11, 0)
    def encode(self):
        return self.args

class Error(ViewServerException):
    def __init__(self, id, reason):
        super(Error, self).__init__('error', id, reason)

class FatalError(ViewServerException):
    def __init__(self, id, reason):
        super(FatalError, self).__init__('error', id, reason)

class Forbidden(ViewServerException):
    def __init__(self, reason):
        super(type(self), self).__init__('forbidden', reason)

    def encode(self):
        forbidden, reason = self.args
        return {forbidden: reason}

def run(input=sys.stdin, output=sys.stdout):
    r"""CouchDB view function handler implementation for Python.

    :param input: the readable file-like object to read input from
    :param output: the writable file-like object to write output to
    """
    def respond(obj):
        try:
            obj = json.encode(obj)
        except ValueError as err:
            log.error('Error converting %r to json', obj, exc_info=True)
            _log('Error converting object to JSON: %s' % err)
            _log('error on obj: %r' % obj)
            raise FatalError('json_encode', str(err))
        else:
            if isinstance(obj, unicode):
                obj = obj.encode('utf-8')
            output.write(obj)
            output.write('\n')
            output.flush()

    @CouchDBVersion.minimal(0, 9, 0)
    def _log(message):
        if message is None:
            message = 'Error: attemting to log message of None'
        if not isinstance(message, basestring):
            message = json.encode(message)
        respond({'log': message})

    @CouchDBVersion.minimal(0, 11, 0)
    def _log(message):
        if not isinstance(message, basestring):
            message = json.encode(message)
        respond(['log', message])

    def compile_func(funstr):
        log.debug('Compiling code to function:\n%s', funstr)
        funstr = BOM_UTF8 + funstr.encode('utf-8')
        globals_ = {}
        try:
            # context is defined below after all classes
            exec funstr in context, globals_
        except Exception as err:
            raise Error('compilation_error', '%s:\n%s' % (err, funstr))
        try:
            func = globals_ and globals_.values()[0] or None
            assert isinstance(func, FunctionType)
        except AssertionError:
            msg = 'Expression does not eval to a function: \n%s' % funstr
            raise Error('compilation_error', msg)
        else:
            return func

    class Mimeparse(object):
        __slots__ = ()

        def __parse_ranges(self, ranges):
            return [self.parse_media_range(item) for item in ranges.split(',')]

        def parse_mimetype(self, mimetype):
            parts = mimetype.split(';')
            params = dict([item.split('=', 2) for item in parts if '=' in item])
            fulltype = parts[0].strip()
            if fulltype == '*':
                fulltype = '*/*'
            typeparts = fulltype.split('/')
            return typeparts[0], typeparts[1], params

        def parse_media_range(self, range):
            parsed_type = self.parse_mimetype(range)
            q = float(parsed_type[2].get('q', '1'))
            if q < 0 or q >= 1:
                parsed_type[2]['q'] = '1'
            return parsed_type

        def fitness_and_quality(self, mimetype, ranges):
            parsed_ranges = self.__parse_ranges(ranges)
            best_fitness = -1
            best_fit_q = 0
            base_type, base_subtype, base_params = self.parse_media_range(mimetype)
            for parsed in parsed_ranges:
                type, subtype, params = parsed
                type_preq = type == base_type or '*' in [type, base_type]
                subtype_preq = subtype == base_subtype or '*' in [subtype, base_subtype]
                if type_preq and subtype_preq:
                    match_count = sum((1 for k, v in base_params.values()
                                      if k != 'q' and params.get(k) == v))
                    fitness = type == base_type and 100 or 0
                    fitness += subtype == base_subtype and 10 or 0
                    fitness += match_count
                    if fitness > best_fitness:
                        best_fitness = fitness
                        best_fit_q = params.get('q', 0)
            return best_fitness, float(best_fit_q)

        def quality(self, mimetype, ranges):
            return self.fitness_and_quality(mimetype, ranges)

        def best_match(self, supported, header):
            weighted = []
            for i, item in enumerate(supported):
                weighted.append(self.fitness_and_quality(item, header), i, item)
            weighted.sort()
            return weighted[-1][0][1] and weighted[-1][2] or ''

    Mimeparse = Mimeparse()

    class Mime(object):
        __slots__ = ('resp_content_type')
        mimes_by_key = {}
        keys_by_mime = {}
        mimefuns = []

        def __init__(self):
            self.reset_provides()
            # Some default types
            # Ported from Ruby on Rails
            # Build list of Mime types for HTTP responses
            # http://www.iana.org/assignments/media-types/
            # http://dev.rubyonrails.org/svn/rails/trunk/actionpack/lib/action_controller/mime_types.rb

            self.register_type('all', '*/*')
            self.register_type('text', 'text/plain; charset=utf-8', 'txt')
            self.register_type('html', 'text/html; charset=utf-8')
            self.register_type('xhtml', 'application/xhtml+xml', 'xhtml')
            self.register_type('xml', 'application/xml', 'text/xml', 'application/x-xml')
            self.register_type('js', 'text/javascript', 'application/javascript', 'application/x-javascript')
            self.register_type('css', 'text/css')
            self.register_type('ics', 'text/calendar')
            self.register_type('csv', 'text/csv')
            self.register_type('rss', 'application/rss+xml')
            self.register_type('atom', 'application/atom+xml')
            self.register_type('yaml', 'application/x-yaml', 'text/yaml')
            # just like Rails
            self.register_type('multipart_form', 'multipart/form-data')
            self.register_type('url_encoded_form', 'application/x-www-form-urlencoded')
            # http://www.ietf.org/rfc/rfc4627.txt
            self.register_type('json', 'application/json', 'text/x-json')

        @property
        def provides_used(self):
            return bool(self.mimefuns)

        def reset_provides(self):
            self.resp_content_type = None
            del self.mimefuns[:]

        def register_type(self, key, * args):
            self.mimes_by_key[key] = args
            for item in args:
                self.keys_by_mime[item] = key

        def provides(self, type, func):
            self.provides_used = True
            self.mimefuns.append((type, func))

        def run_provides(self, req):
            supported_mimes = []
            bestfun = None
            bestkey = None
            accept = req.headers['Accept']
            if req.query and req.query.format:
                bestkey = req.query.format
                self.resp_content_type = self.mimes_by_key[bestkey][0]
            elif accept:
                for mimefun in reversed(self.mimefuns):
                    mimekey = mimefun[0]
                    if self.mimes_by_key.get(mimekey) is not None:
                        supported_mimes.extend(self.mimes_by_key[mimekey])
                self.resp_content_type = Mimeparse.best_match(supported_mimes, accept)
            else:
                bestkey = self.mimefuns[0][0]
            if bestkey is not None:
                for item in self.mimefuns:
                    if item[0] == bestkey:
                        bestfun = item[1]
                        break
            if bestfun is not None:
                return bestfun()
            supported_types = [', '.join(value) or key
                               for key, value in self.mimes_by_key.values()]
            raise Error('not_acceptable',
                        'Content-Type %s not supported, try one of:\n'
                        '%s' % (accept or bestkey, ', '.join(supported_types)))

    Mime = Mime()

    class State(object):
        __slots__ = ('line_length')
        functions = []
        functions_src = []
        query_config = {}

        def reset(self, config=None):
            del self.functions[:]
            self.query_config.clear()
            if config is not None:
                self.query_config.update(config)
            return True

        def add_fun(self, string):
            self.functions.append(compile_func(string))
            self.functions_src.append(string)
            return True

    State = State()

    class Views(object):
        __slots__ = ()

        def map_doc(self, doc):
            docid = doc.get('_id')
            log.debug('Running map functions for doc._id %s', docid)
            map_results = []
            orig_doc = doc.copy()
            for i, function in enumerate(State.functions):
                try:
                    result = [[key, value] for key, value in function(doc)]
                except ViewServerException:
                    raise
                except Exception as err:
                    # javascript view server allows us to keep silence for non
                    # fatal errors, but I don't see any reason to do same thing
                    # for python view server because it's not Zen way and
                    # without log monitoring there will be no way to get to know
                    # about such errors. Let it crush! :)
                    msg = 'map function raised error for doc._id %s\n%s\n'
                    funstr = State.functions_src[i]
                    log.error(msg, docid, funstr, exc_info=True)
                    raise FatalError(type(err).__name__, msg % (docid, err))
                else:
                    map_results.append(result)
                # quick and dirty trick to prevent document from changing
                # within map functions.
                # this seal was removed from javascript view-server, but
                # I don't see any reasons to leave same behavior.
                if doc != orig_doc:
                    log.warn("Document %s had been changed by map function "
                             "'%s', but was restored to original state",
                             doc.get('_id'), function.__name__)
                    doc = orig_doc.copy()
            return map_results

        def reduce(self, reduce_funs, kvs, rereduce=False):
            reductions = []
            keys, values = rereduce and (None, kvs) or zip(*kvs)
            args = (keys, values, rereduce)
            for funstr in reduce_funs:
                try:
                    function = compile_func(funstr)
                    result = function(*args[:function.func_code.co_argcount])
                except ViewServerException:
                    raise
                except Exception as err:
                    # see comments for same block at map_doc
                    msg = 'reduce function raised error:\n%s' % err
                    log.error(msg, exc_info=True)
                    raise Error(type(err).__name__, msg)
                else:
                    reductions.append(result)

            reduce_len = len(reductions)
            reduce_limit = State.query_config.get('reduce_limit', False)
            size_overflowed = (reduce_len * 2) > State.line_length

            if reduce_limit and reduce_len > 200  and size_overflowed:
                reduce_line = json.encode(reductions)
                msg = "Reduce output must shirnk more rapidly:\n"\
                      "Current output: '%s'... "\
                      "(first 100 of %d bytes)" % (reduce_line[:100], reduce_len)
                raise Error('reduce_overflow_error', msg)
            return [True, reductions]

        def rereduce(self, reduce_funs, values):
            return self.reduce(reduce_funs, values, rereduce=True)

    Views = Views()

    class Validate(object):
        __slots__ = ()

        def handle_error(self, err, userctx):
            if isinstance(err, Forbidden):
                reason = err.args[1]
                log.warn('Access deny for user %s. Reason: %s', userctx, reason)
                raise
            elif isinstance(err, AssertionError):
                # This is custom behavior that allows to use assert statement
                # for field validation. It's just quite handy.
                log.warn('Access deny for user %s. Reason: %s', userctx, err)
                raise Forbidden(str(err))

        def run_validate(self, func, newdoc, olddoc, userctx):
            try:
                func(newdoc, olddoc, userctx)
            except (AssertionError, Forbidden) as err:
                self.handle_error(err, userctx)
            return 1

        @CouchDBVersion.minimal(0, 9, 0)
        def validate(self, funstr, *args):
            func = compile_func(funstr)
            return self.run_validate(func, *args)

        @CouchDBVersion.minimal(0, 11, 0)
        def validate(self, func, *args):
            # occured at least for 0.11.1 - forth argument empty dict
            # what is it?
            return self.run_validate(func, *args[:3])

    Validate = Validate()

    class Filters(object):
        __slots__ = ()

        def run_filter(self, func, docs, req, userctx=None):
            # userctx may not been passed
            return [True, [bool(func(doc, req, userctx)) for doc in docs]]

        @CouchDBVersion.minimal(0, 10, 0)
        def filter(self, *args):
            return self.run_filter(State.functions[0], *args)

        @CouchDBVersion.minimal(0, 11, 0)
        def filter(self, func, *args):
            return self.run_filter(func, *args)

    Filters = Filters()

    class Render(object):
        __slots__ = ('gotrow', 'lastrow')
        chunks = []
        startresp = {}

        def __init__(self):
            self.reset_list()

        def reset_list(self):
            del self.chunks[:]
            self.startresp.clear()
            self.gotrow = False
            self.lastrow = False

        def start(self, resp=None):
            self.startresp.clear()
            self.startresp.update(resp or {})

        def send_start(self):
            resp = self.apply_content_type(self.startresp or {},
                                           Mime.resp_content_type)
            respond(['start', self.chunks, resp])
            del self.chunks[:]
            self.startresp.clear()

        def apply_content_type(self, resp, resp_content_type):
            if not resp.get('headers'):
                resp['headers'] = {}
            if resp_content_type and not resp['headers'].get('Content-Type'):
                resp['headers']['Content-Type'] = resp_content_type
            return resp

        def send(self, chunk):
            self.chunks.append(unicode(chunk))

        def blow_chunks(self, label='chunks'):
            respond([label, self.chunks])
            del self.chunks[:]

        def get_row(self):
            while True:
                if self.lastrow:
                    break
                if not self.gotrow:
                    self.gotrow = True
                    self.send_start()
                else:
                    self.blow_chunks()
                line = input.readline()
                if not line:
                    break
                try:
                    data = json.decode(line)
                except ValueError as err:
                    log.error('Error converting JSON to object: %s\n'
                              'Reason: %s', line, err, exc_info=True)
                    raise FatalError('json_decode_error', str(err))
                if data[0] == 'list_end':
                    self.lastrow = True
                    break
                if data[0] != 'list_row':
                    raise FatalError('list_error', 'not a row `%s`' % data[0])
                yield data[1]

        def maybe_wrap_response(self, resp):
            if isinstance(resp, basestring):
                return {'body': resp}
            else:
                return resp

        def is_doc_request_path(self, info):
            return len(info.path) > 5

        def run_show(self, fun, *args):
            log.debug('Running show function %s with args %s',
                      fun.func_name, args)
            try:
                self.reset_list()
                Mime.reset_provides()
                resp = fun(*args) or {}
                if self.chunks:
                    resp = self.maybe_wrap_response(resp)
                    resp['headers'] = resp['headers'] or {};
                    resp.headers.update(self.startresp)
                    resp['body'] = ''.join(self.chunks) + resp.get('body', '')
                    self.reset_list()
                if Mime.provides_used:
                    resp = Mime.run_provides(args[1])
                    resp = self.maybe_wrap_response(resp)
                    resp = self.apply_content_type(resp, Mime.resp_content_type)
                if isinstance(resp, (dict, basestring)):
                    respond(['resp', self.maybe_wrap_response(resp)])
                else:
                    log.debug('resp: %r ; type: %r', resp, type(resp))
                    raise Error('render_error',
                                'undefined response from show function')
            except Exception as err:
                if args[0] is None and self.is_doc_request_path(args[1]):
                    raise Error('not_found', 'document not found')
                if isinstance(err, ViewServerException):
                    raise
                else:
                    log.error('unexpected error occured', exc_info=True)
                    raise Error('render_error', str(err))

        def run_update(self, fun, *args):
            log.debug('Running update function %s with args %s',
                      fun.func_name, args)
            try:
                method = args[1]['method']
                if method == 'GET':
                    log.debug('method: %s', method)
                    raise Error('error', 'method_not_allowed',
                                'Update functions do not allow GET')
                doc, resp = fun(*args)
                if isinstance(resp, (dict, basestring)):
                    respond(['up', doc, self.maybe_wrap_response(resp)])
                else:
                    log.debug('resp: %r ; type: %r', resp, type(resp))
                    raise Error('render_error',
                                'undefined response from update function')
            except ViewServerException:
                raise
            except Exception as err:
                log.error('unexpected error occured', exc_info=True)
                raise Error('render_error', str(err))

        def run_list(self, fun, *args):
            log.debug('Running list function %s with args %s',
                      fun.func_name, args)
            try:
                Mime.reset_provides()
                self.reset_list()
                head, req = args
                tail = fun(*args)
                if Mime.provides_used:
                    tail = Mime.run_provides(req)
                if not self.gotrow:
                    for row in self.get_row():
                        break
                if tail is not None:
                    self.chunks.append(tail)
                self.blow_chunks('end')
            except ViewServerException:
                raise
            except Exception as err:
                log.error('unexpected error occured', exc_info=True)
                raise Error('render_error', str(err))

        @CouchDBVersion.minimal(0, 10, 0)
        def list(self, *args):
            return self.run_list(State.functions[0], *args)

        @CouchDBVersion.minimal(0, 11, 0)
        def list(self, func, *args):
            return self.run_list(func, *args)

        @CouchDBVersion.minimal(0, 10, 0)
        def show(self, funstr, *args):
            func = compile_func(funstr)
            return self.run_show(func, *args)

        @CouchDBVersion.minimal(0, 11, 0)
        def show(self, func, *args):
            return self.run_show(func, *args)

        @CouchDBVersion.minimal(0, 10, 0)
        def update(self, funstr, *args):
            func = compile_func(funstr)
            return self.run_update(func, *args)

        @CouchDBVersion.minimal(0, 11, 0)
        def update(self, func, *args):
            return self.run_update(func, *args)

    Render = Render()

    # for 0.9 only
    class RenderOld(object):
        __slots__ = ()
        row_line = {}

        def render_function(self, func, args, funstr=None, html_errors=False):
            try:
                resp = func(*args)
                if resp:
                    return Render.maybe_wrap_response(resp)
                else:
                    raise Error('render_error', 'undefined response from render'
                                                'function: %s' % resp)
            except Exception as err:
                _log('function raised error: %s' % err)
                _log('stacktrace: %s' % traceback.format_exc())
                if html_errors:
                    msg = self.html_render_error(err, funstr)
                    raise Error('render_error', msg)
                raise

        def response_with(self, req, responders):
            best_mime = 'text/plain'
            best_key = None
            accept = req.get('Accept', None)
            if accept is not None and not 'format' in req['query']:
                provides = []
                for key in responders:
                    if key in Mime.mimes_by_key:
                        provides.append(Mime.mimes_by_key[key])
                best_mime = Mimeparse.best_match(Mime.mimes_by_key[key], accept)
                best_key = Mime.keys_by_mime[best_mime]
            else:
                best_key = req['query'].get('format')
            rfunc = responders.get(best_key or responders.get('fallback') or 'html')
            if rfunc is not None:
                resp = Render.maybe_wrap_response(rfunc())
                if not 'headers' in resp:
                    resp['headers'] = {}
                resp['headers']['Content-Type'] = best_mime
                return resp
            else:
                return {'code': 406, 'body': 'Not Acceptable: %s' % accept}

        def show_doc(self, funstr, doc, req=None):
            func = compile_func(funstr)
            return self.render_function(func, [doc, req])

        def list_begin(self, head, req):
            func = State.functions[0]
            self.row_line[func] = {
                'first_key': None,
                'row_number': 0,
                'prev_key': None
            }
            return self.render_function(func, [head, None, req, None])

        def list_row(self, row, req):
            func = State.functions[0]
            funstr = State.functions_src[0]
            row_info = self.row_line.get(func, None)
            assert row_info is not None
            resp = self.render_function(func, [None, row, req, row_info],
                                        funstr, True)
            if row_info['first_key'] is None:
                row_info['first_key'] = row.get('key')
            row_info['prev_key'] = row.get('key')
            row_info['row_number'] += 1
            self.row_line[func] = row_info
            return resp

        def list_tail(self, req):
            func = State.functions[0]
            row_info = self.row_line.pop(func, None)
            return self.render_function(func, [None, None, req, row_info])

        def html_render_error(self, err, funstr):
            import cgi
            return {
                'body':''.join([
                '<html><body><h1>Render Error</h1>',
                str(err),
                '</p><h2>Stacktrace:</h2><code><pre>',
                traceback.format_exc(),
                '</pre></code><h2>Function source:</h2><code><pre>',
                funstr])
            }

    RenderOld = RenderOld()

    class DDoc(object):
        __slots__ = ()
        ddocs = {}

        @CouchDBVersion.minimal(0, 11, 0)
        def dispatcher(self):
            return {
                'lists': Render.list,
                'shows': Render.show,
                'filters': Filters.filter,
                'updates': Render.update,
                'validate_doc_update': Validate.validate
            }

        def ddoc(self, *_args):
            dispatch = self.dispatcher()
            args = list(_args)
            ddoc_id = args.pop(0)
            if ddoc_id == 'new':
                ddoc_id = args.pop(0)
                self.ddocs[ddoc_id] = args.pop(0)
                return True
            else:
                ddoc = self.ddocs.get(ddoc_id)
                if ddoc is None:
                    raise FatalError('query_protocol_error',
                                     'uncached design doc: %s' % ddoc_id)
                fun_path = args.pop(0)
                cmd = fun_path[0]
                func_args = args.pop(0)
                if dispatch.get(fun_path[0]) is None:
                    raise FatalError('unknown_command',
                                     'unknown ddoc command `%s`' % cmd)
                point = ddoc
                for item in fun_path:
                    point = point[item]
                else:
                    func = point
                    if type(func) is not FunctionType:
                        func = compile_func(func)
                return dispatch[cmd](func, *func_args)
            return 1

    DDoc = DDoc()

    @CouchDBVersion.minimal(0, 9, 0)
    def context_extension():
        return {'response_with': RenderOld.response_with}

    @CouchDBVersion.minimal(0, 10, 0)
    def context_extension():
        return {'start': Render.start,
                'send': Render.send,
                'get_row': Render.get_row}

    context = {
        'log': _log,
        'provides': Mime.provides,
        'register_type': Mime.register_type,
        'json': json,
        'Forbidden': Forbidden,
        'Error': Error,
        'FatalError': FatalError}
    context.update(context_extension())

    @CouchDBVersion.minimal(0, 9, 0)
    def handlers_extension():
        return {'show_doc': RenderOld.show_doc,
                'list_begin': RenderOld.list_begin,
                'list_row': RenderOld.list_row,
                'list_tail': RenderOld.list_tail,
                'validate': Validate.validate}

    @CouchDBVersion.minimal(0, 10, 0)
    def handlers_extension():
        return {'list': Render.list,
                'show': Render.show,
                'filter': Filters.filter,
                'update': Render.update,
                'validate': Validate.validate}

    @CouchDBVersion.minimal(0, 11, 0)
    def handlers_extension():
        return {'ddoc': DDoc.ddoc}

    handlers = {
        'reset': State.reset,
        'add_fun': State.add_fun,
        'map_doc': Views.map_doc,
        'reduce': Views.reduce,
        'rereduce': Views.rereduce}
    handlers.update(handlers_extension())

    try:
        while True:
            line = input.readline()
            State.line_length = len(line)
            if not line:
                break
            cmd = json.decode(line)
            log.debug('Processing %r', cmd)
            try:
                if not cmd[0] in handlers:
                    raise FatalError('unknown_command',
                                     'unknown command %s' % cmd[0])
                retval = handlers[cmd[0]](*cmd[1:])
            except FatalError as err:
                log.critical('FatalError occured %s: %s',
                             *err.args[1:],
                             exc_info=True)
                respond(err.encode())
                return 1
            except Forbidden as err:
                reason = err.args[1]
                log.warn('ForbiddenError occured: %s', reason)
                respond(err.encode())
            except Error as err:
                log.error('Error occured %s: %s',
                          *err.args[1:],
                          exc_info=True)
                respond(err.encode())
            except Exception as err:
                err_name = type(err).__name__
                err_msg = str(err)
                log.critical('%s: %s', err_name, err_msg, exc_info=True)
                respond(['error', err_name, err_msg])
                return 1
            else:
                log.debug('Returning  %r', retval)
                respond(retval)
    except KeyboardInterrupt:
        return 0
    except Exception as err:
        log.critical('Unexpected error occured: %s', err, exc_info=True)
        return 1


_VERSION = """%(name)s - CouchDB Python %(version)s

Copyright (C) 2007 Christopher Lenz <cmlenz@gmx.de>.
"""

_HELP = """Usage: %(name)s [OPTION]

The %(name)s command runs the CouchDB Python view server.

The exit status is 0 for success or 1 for failure.

Options:

  --version               display version information and exit
  -h, --help              display a short help message and exit
  --json-module=<name>    set the JSON module to use ('simplejson', 'cjson',
                          or 'json' are supported)
  --log-file=<file>       name of the file to write log messages to, or '-' to
                          enable logging to the standard error stream
  --debug                 enable debug logging; requires --log-file to be
                          specified
  --couchdb-version=<ver> define with which version of couchdb server will work
                          default: latest one.

Report bugs via the web at <http://code.google.com/p/couchdb-python>.
"""


def main():
    """Command-line entry point for running the view server."""
    import getopt
    from couchdb import __version__ as VERSION

    try:
        option_list, argument_list = getopt.gnu_getopt(
            sys.argv[1:], 'h',
            ['version', 'help', 'json-module=', 'debug', 'log-file=',
             'couchdb-version=']
        )
        _run_version = 'latest'
        message = None
        for option, value in option_list:
            if option in ['--version']:
                message = _VERSION % dict(name=os.path.basename(sys.argv[0]),
                                      version=VERSION)
            elif option in ['-h', '--help']:
                message = _HELP % dict(name=os.path.basename(sys.argv[0]))
            elif option in ['--json-module']:
                json.use(module=value)
            elif option in ['--debug']:
                log.setLevel(logging.DEBUG)
            elif option in ['--log-file']:
                if value == '-':
                    handler = logging.StreamHandler(sys.stderr)
                    handler.setFormatter(logging.Formatter(
                        ' -> [%(levelname)s] %(message)s'
                    ))
                else:
                    handler = logging.FileHandler(value)
                    handler.setFormatter(logging.Formatter(
                        '[%(asctime)s] [%(levelname)s] %(message)s'
                    ))
                log.addHandler(handler)
            elif option in ['--couchdb-version']:
                version = value.split('.')
                while len(version) < 3:
                    version.append(0)
                CouchDBVersion.current = tuple(map(int, version[:3]))
                _run_version = '.'.join(version[:3])
        if message:
            sys.stdout.write(message)
            sys.stdout.flush()
            sys.exit(0)

    except getopt.GetoptError, error:
        message = '%s\n\nTry `%s --help` for more information.\n' % (
            str(error), os.path.basename(sys.argv[0])
        )
        sys.stderr.write(message)
        sys.stderr.flush()
        sys.exit(1)
    log.info('View server started for CouchDB %s version' % _run_version)
    sys.exit(run())


if __name__ == '__main__':
    main()
