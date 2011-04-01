#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2008 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Implementation of a view server for functions written in Python."""
import logging
import os
import sys
import traceback
from codecs import BOM_UTF8
from types import FunctionType

from couchdb import json

__all__ = ['main', 'run']
__docformat__ = 'restructuredtext en'

TRUNK = (999, 999, 999) # assume latest one

class NullHandler(logging.Handler):
    def emit(self, *args, **kwargs):
        pass

log = logging.getLogger('couchdb.view')
log.addHandler(NullHandler())

def run(input=sys.stdin, output=sys.stdout, version=TRUNK):
    r"""CouchDB view server implementation for Python.

    :param input: the readable file-like object to read input from
    :param output: the writable file-like object to write output to
    :param version: three element tuple with represents couchdb server version
                    number.
    """
    assert version <= TRUNK
    COUCHDB_VERSION = version

################################################################################
# Exceptions
#
    class ViewServerException(Exception):
        '''Base view server exception'''
        def encode(self):
            '''Encodes error to valid output structure.

            Returns:
                For version lesser than 0.11.0 valid format is a dict with
                structure:
                {'error': id, 'reason': message}
                Since 0.11.0 error format had changed to list:
                ['error', id, message]
            '''
            if COUCHDB_VERSION < (0, 11, 0):
                id, reason = self.args
                return {'error': id, 'reason': reason}
            else:
                return ['error'] + list(self.args)

    class Error(ViewServerException):
        '''Non fatal error which doesn't initiate view server termitation.'''

    class FatalError(ViewServerException):
        '''Fatal error which termitates view server.'''

    class Forbidden(ViewServerException):
        '''Non fatal error which signs operation access deny.'''
        def encode(self):
            return {'forbidden': self.args[0]}

################################################################################
# Helpers
#

    def debug_dump_args(func):
        ''''Decorator which traces function call and logs passed arguments.'''
        argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
        fname = func.func_name
        def wrapper(*args,**kwargs):
            msg = 'Called `' + fname + '` with args:\n' + '\n'.join(
                    '  %s = %s' % entry
                    for entry in zip(argnames,args) + kwargs.items())
            log.debug(msg)
            return func(*args, **kwargs)
        return wrapper

################################################################################
# Common functions
#

    @debug_dump_args
    def respond(obj):
        '''Writes json encoded object to view server output stream.'''
        try:
            obj = json.encode(obj)
        except ValueError, err:
            log.exception('Error converting %r to json', obj)
            _log('Error converting object to JSON: %s' % err)
            _log('error on obj: %r' % obj)
            raise FatalError('json_encode', str(err))
        else:
            if isinstance(obj, unicode):
                obj = obj.encode('utf-8')
            output.write(obj)
            output.write('\n')
            output.flush()

    def _log(message):
        '''Logs message to CouchDB output stream.

        Output format:
            till 0.11.0 version: {"log": message}
            since 0.11.0 version: ["log", message]
        '''
        if COUCHDB_VERSION < (0, 11, 0):
            if message is None:
                message = 'Error: attemting to log message of None'
            if not isinstance(message, basestring):
                message = json.encode(message)
            respond({'log': message})
        else:
            if not isinstance(message, basestring):
                message = json.encode(message)
            respond(['log', message])

    @debug_dump_args
    def resolve_module(names, mod, root=None):
        def helper():
            return ('\n    id: %r'
                    '\n    names: %r'
                    '\n    parent: %r'
                    '\n    current: %r'
                    '\n    root: %r') % (id, names, parent, current, root)
        id = mod.get('id')
        parent = mod.get('parent')
        current = mod.get('current')
        if not names:
            if not isinstance(current, basestring):
                raise Error('invalid_require_path',
                            'Must require Python string, not\n%r' % current)
            return {
                'current': current,
                'parent': parent,
                'id': id,
                'exports': {}
            }
        n = names.pop(0)
        if n == '..':
            if parent is None or parent.get('parent') is None:
                raise Error('invalid_require_path',
                            'Object %r has no parent.' % id + helper())
            return resolve_module(names, {
                'id': id[:id.rfind('/')],
                'parent': parent['parent'].get('parent'),
                'current': parent['parent'].get('current')
            })
        elif n == '.':
            if parent is None:
                raise Error('invalid_require_path',
                            'Object %r has no parent.' % id + helper())
            return resolve_module(names, {
                'id': id,
                'parent': parent.get('parent'),
                'current': parent.get('current'),
            })
        elif not n:
            raise Error('invalid_require_path',
                        'Required path shouldn\'t starts with slash character'
                        ' or contains sequence of slashes.' + helper())
        elif root:
            mod, current = {'current': root}, root
        if current is None:
            raise Error('invalid_require_path',
                        'Required module missing.' + helper())
        if not n in current:
            raise Error('invalid_require_path',
                        'Object %r has no property %r' % (id, n) + helper())
        return resolve_module(names, {
            'current': current[n],
            'parent': mod,
            'id': (id is not None) and (id + '/' + n) or n
        })

    def _require(ddoc):
        @debug_dump_args
        def require(name, module=None):
            '''Extracts export statements from stored module within document.

            Arguments:
                name: Path to stored module throught document structure fields.

            Returns:
                Exported statements.

            Example of stored module:
                >>> class Validate(object):
                >>>     def __init__(self, newdoc, olddoc, userctx):
                >>>         self.newdoc = newdoc
                >>>         self.olddoc = olddoc
                >>>         self.userctx = userctx
                >>>
                >>>     def is_author():
                >>>         return self.doc['author'] == self.userctx['name']
                >>>
                >>>     def is_admin():
                >>>         return '_admin' in self.userctx['roles']
                >>>
                >>>     def unchanged(field):
                >>>         assert (self.olddoc is not None
                >>>                 and self.olddoc[field] == self.newdoc[field])
                >>>
                >>> exports['init'] = Validate
            Example of importing exports:
                >>> def validate_doc_update(newdoc, olddoc, userctx):
                >>>     init_v = require('lib/validate')['init']
                >>>     v = init_v(newdoc, olddoc, userctx)
                >>>
                >>>     if v.is_admin():
                >>>         return True
                >>>
                >>>     v.unchanged('author')
                >>>     v.unchanged('created_at')
                >>>     return True
            '''
            log.debug('Importing objects from %s', name)
            module = module or {}
            new_module = resolve_module(name.split('/'), module, ddoc)
            source = new_module['current']
            globals_ = {
                'module': new_module,
                'exports': new_module['exports'],
            }
            module_context = context.copy()
            module_context['require'] = lambda name: require(name, new_module)
            try:
                bytecode = compile(source, '<string>', 'exec')
                exec bytecode in module_context, globals_
            except Exception, err:
                raise Error('compilation_error', '%s:\n%s' % (err, source))
            else:
                return globals_['exports']
        return require

    def compile_func(funstr, ddoc=None):
        '''Compile source code and extract function object from it.

        Code compiled within special context which provides access to predefined
        objects and functions:
            Error: Non fatal view server exception.
            FatalError: Fatal view server exception.
            Forbidden: Non fatal view server exception for access control.
            log: Method to log messages to view server output stream.
            json: View server json package (simplejson, cjson or json)

        If ddoc argument passed (since 0.11.0):
            require: Code import helper stored in various document sections.
                Not avaiable for map/reduce functions.
                Since 1.1.0 avaiable for map functions if State.lib setted.

        Useful for show/list functions:
            provides: Register mime type handler.
            register_type: Register new mime type.

            Only for 0.9.x:
                response_with

            Since 0.10.0:
                start: Initiate response with passed headers.
                send: Sends single chunk to caller.
                get_row: Gets next row from view result.

        Arguments:
            funstr: Python source code.
            ddoc: Optional argument which must represent document as dict.

        Returns:
            Compiled function object.

        Raises:
            Error: View server error if compilation was not succeeded.
        '''
        # context is defined below after all classes
        context.pop('require', None) # cleanup
        log.debug('Compiling code to function:\n%s', funstr)
        funstr = BOM_UTF8 + funstr.encode('utf-8')
        globals_ = {}
        if ddoc is not None:
            context['require'] = _require(ddoc)
        try:
            # compile + exec > exec
            bytecode = compile(funstr, '<string>', 'exec')
            exec bytecode in context, globals_
        except Exception, err:
            raise Error('compilation_error', '%s:\n%s' % (err, funstr))
        try:
            func = globals_ and globals_.values()[0] or None
            assert isinstance(func, FunctionType)
        except AssertionError:
            msg = 'Expression does not eval to a function: \n%s' % funstr
            raise Error('compilation_error', msg)
        else:
            return func

################################################################################
# Mimeparse
#
# TODO: Is there way to do same things with builtin modules?

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
                subtype_preq = subtype == base_subtype or '*' in [subtype,
                                                                  base_subtype]
                if type_preq and subtype_preq:
                    match_count = sum((1 for k, v in base_params.items()
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
                weighted.append([self.fitness_and_quality(item, header), i, item])
            weighted.sort()
            return weighted[-1][0][1] and weighted[-1][2] or ''

    Mimeparse = Mimeparse()

################################################################################
# Mime
#

    class Mime(object):
        __slots__ = ('resp_content_type',)
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
            types = {
                'all': ['*/*'],
                'text': ['text/plain; charset=utf-8', 'txt'],
                'html': ['text/html; charset=utf-8'],
                'xhtml': ['application/xhtml+xml', 'xhtml'],
                'xml': ['application/xml', 'text/xml', 'application/x-xml'],
                'js': ['text/javascript', 'application/javascript',
                    'application/x-javascript'],
                'css': ['text/css'],
                'ics': ['text/calendar'],
                'csv': ['text/csv'],
                'rss': ['application/rss+xml'],
                'atom': ['application/atom+xml'],
                'yaml': ['application/x-yaml', 'text/yaml'],
                # just like Rails
                'multipart_form': ['multipart/form-data'],
                'url_encoded_form': ['application/x-www-form-urlencoded'],
                # http://www.ietf.org/rfc/rfc4627.txt
                'json': ['application/json', 'text/x-json']
            }
            for k, v in types.items():
                self.register_type(k, *v)

        @property
        def provides_used(self):
            return bool(self.mimefuns)

        def reset_provides(self):
            self.resp_content_type = None
            del self.mimefuns[:]

        def register_type(self, key, *args):
            '''Register mimetypes.

            Predefined types:
                all: */*
                text: text/plain; charset=utf-8, txt
                html: text/html; charset=utf-8
                xhtml: application/xhtml+xml, xhtml
                xml: application/xml, text/xml, application/x-xml
                js: text/javascript, application/javascript,
                    application/x-javascript
                css: text/css
                ics: text/calendar
                csv: text/csv
                rss: application/rss+xml
                atom: application/atom+xml
                yaml: application/x-yaml, text/yaml
                multipart_form: multipart/form-data
                url_encoded_form: application/x-www-form-urlencoded
                json: application/json, text/x-json

            Arguments:
                key: Shorthand key for mimetypes. Actually you would like
                    to use extension name associated with mime types e.g. js
                *args: full quality names of mime types.

            Example:
                >>> register_type('png', 'image/png')
            '''
            self.mimes_by_key[key] = args
            for item in args:
                self.keys_by_mime[item] = key

        def provides(self, type, func):
            '''Register mimetype handler which will be called when mimetype been
            requested.

            Arguments:
                type: Mimetype.
                func: Function object or any callable.
            '''
            self.mimefuns.append((type, func))

        def run_provides(self, req):
            supported_mimes = []
            bestfun = None
            bestkey = None

            accept = None
            if 'headers' in req:
                accept = req['headers'].get('Accept')
            if 'query' in req and 'format' in req['query']:
                bestkey = req['query']['format']
                self.resp_content_type = self.mimes_by_key[bestkey][0]
            elif accept:
                for mimefun in reversed(self.mimefuns):
                    mimekey = mimefun[0]
                    if self.mimes_by_key.get(mimekey) is not None:
                        supported_mimes.extend(self.mimes_by_key[mimekey])
                self.resp_content_type = Mimeparse.best_match(supported_mimes,
                                                              accept)
                bestkey = self.keys_by_mime.get(self.resp_content_type)
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
                               for key, value in self.mimes_by_key.items()]
            raise Error('not_acceptable',
                        'Content-Type %s not supported, try one of:\n'
                        '%s' % (accept or bestkey, ', '.join(supported_types)))

    Mime = Mime()

################################################################################
# State
#

    class State(object):
        '''View server state holder.

        Attributes:
            lib: Shared module for views. Feature of 1.1.0 version.
            functions: List of compiled functions.
            functions_src: List of compiled functions source code.
            query_config: Query config dictionary.
        '''
        __slots__ = ('line_length',)
        lib = None
        functions = []
        functions_src = []
        query_config = {}

        def reset(self, config=None):
            '''Resets view server state.

            Command:
                reset

            Arguments:
                config: Optional dict argument to set up query config.

            Returns:
                True
            '''
            del self.functions[:]
            self.query_config.clear()
            if config is not None:
                self.query_config.update(config)
            return True

        @debug_dump_args
        def add_fun(self, funstr):
            '''Compiles and adds function to state cache.

            Since 1.1.0 if add_lib command executed and lib is setted, allows
            to adds require function to compilation context.

            Command:
                add_fun

            Arguments:
                funstr: Python function as source string.

            Returns:
                True
            '''
            if (1, 1, 0) <= COUCHDB_VERSION <= TRUNK:
                ddoc = {'views': {'lib': self.lib}}
                self.functions.append(compile_func(funstr, ddoc))
            else:
                self.functions.append(compile_func(funstr))
            self.functions_src.append(funstr)
            return True

        @debug_dump_args
        def add_lib(self, lib):
            '''Add lib to state which could be used within views.

            Command:
                add_lib

            Arguments:
                lib: Python source code which used require function protocol.

            Returns:
                True
            '''
            type(self).lib = lib;
            return True

    State = State()

################################################################################
# Views
#

    class Views(object):
        __slots__ = ()

        @debug_dump_args
        def map_doc(self, doc):
            '''Apply avaiable map functions to document.

            Command:
                map_doc

            Arguments:
                doc: Document object as dict.

            Returns:
                List of key-value results for each applied map function.

            Raises:
                FatalError: If any Python exception occures due mapping.

            Example of map function:
                >>> def mapfun(doc):
                >>>     doc_has_tags = isinstance(doc.get('tags'), list)
                >>>     if doc['type'] == 'post' and doc_has_tags:
                >>>         for tag in doc['tags']:
                >>>             yield tag.lower(), 1
            '''
            docid = doc.get('_id')
            log.debug('Running map functions for doc._id %s', docid)
            map_results = []
            orig_doc = doc.copy()
            for i, function in enumerate(State.functions):
                try:
                    result = [[key, value] for key, value in function(doc) or []]
                except ViewServerException:
                    raise
                except Exception, err:
                    # javascript view server allows us to keep silence for non
                    # fatal errors, but I don't see any reason to do same thing
                    # for python view server because it's not Zen way and
                    # without log monitoring there will be no way to get to know
                    # about such errors. Let it crush! :)
                    msg = 'map function raised error for doc._id %s\n%s\n'
                    funstr = State.functions_src[i]
                    log.exception(msg, docid, funstr)
                    raise FatalError(err.__class__.__name__, msg % (docid, err))
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

        @debug_dump_args
        def reduce(self, reduce_funs, kvs, rereduce=False):
            '''Reduce mapping result.

            Command:
                reduce

            Arguments:
                reduce_funs: List of reduce functions source code.
                kvs: List of key-value pairs.

            Returns:
                Two element list with True and reduction result.

            Raises:
                Error: If any Python exception occures.
                       If reduce ouput is twise longer as State.line_length
                       and reduce_limit is enabled in State.query_config.
            Example of reduce function:
                >>> def reducefun(keys, values):
                >>>     return sum(values)
                Also you may fill free to use builin functions instead:
                >>> _sum
            '''
            reductions = []
            keys, values = rereduce and (None, kvs) or zip(*kvs) or ([],[])
            args = (keys, values, rereduce)
            for funstr in reduce_funs:
                try:
                    function = compile_func(funstr)
                    result = function(*args[:function.func_code.co_argcount])
                except ViewServerException:
                    raise
                except Exception, err:
                    # see comments for same block at map_doc
                    msg = 'reduce function raised error:\n%s' % err
                    log.exception(msg)
                    raise Error(err.__class__.__name__, msg)
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

        @debug_dump_args
        def rereduce(self, reduce_funs, values):
            '''Rereduce mapping result

            Command:
                rereduce

            Arguments:
                reduce_funs: List of reduce functions source code.
                values: List values.

            Returns:
                Two element list with True and rereduction result.

            Raises:
                Error: If any Python exception occures. View server
                    exceptions (Error, FatalError, Forbidden) rethrowing as is.
                    If reduce ouput is twise longer as State.line_length and
                    reduce_limit is enabled in State.query_config.
            '''
            return self.reduce(reduce_funs, values, rereduce=True)

    Views = Views()

################################################################################
# Validate
#
    class Validate(object):
        '''Base class for validation commands.'''
        __slots__ = ()

        def handle_error(self, err, userctx):
            if isinstance(err, Forbidden):
                reason = err.args[0]
                log.warn('Access deny for user %s. Reason: %s', userctx, reason)
                raise
            elif isinstance(err, AssertionError):
                # This is custom behavior that allows to use assert statement
                # for field validation. It's just quite handy.
                log.warn('Access deny for user %s. Reason: %s', userctx, err)
                raise Forbidden(str(err))

        @debug_dump_args
        def run_validate(self, func, *args):
            try:
                func(*args)
            except (AssertionError, Forbidden), err:
                self.handle_error(err, args[2])
            return 1

        def validate(self, func, *args):
            '''Uses for validate_doc_update design function.

            Introduced in 0.9.0 version.
            Should raise Forbidden error to prevent document changes.
            Assertion error counting same as Forbidden error.

            Since 0.11.0 version there was added 4th argument: secobj,
            which holds security information for current database.
            As far as it haven't mentioned in most validate_doc_update examples
            and this change could break old/ported code, this argument leaved
            as optional.

            Command:
                validate

            Arguments:
                func: Validate_doc_update function.
                    Till 0.11.0: function source code.
                    Since 0.11.0: compiled function object.
                newdoc: New document version as dict.
                olddoc: Stored document version as dict.
                userctx: User info dict.
                secobj: Database security information dict.
                    Used since 0.11.0 version. Optional.

            Returns:
                1 (number one)

            Example of validate_doc_update function:
                >>> def validate_doc_update(newdoc, olddoc, userctx, secobj):
                >>>     # of course you should also check roles
                >>>     if userctx['name'] not in secobj['admins']:
                >>>         assert newdoc['author'] == userctx['name']
                >>>     return True
            '''
            if COUCHDB_VERSION < (0, 11, 0):
                func = compile_func(func)
            if COUCHDB_VERSION >= (0, 11, 1):
                argcount = func.func_code.co_argcount == 4 and 4 or 3
                args = args[:argcount]
            return self.run_validate(func, *args)

    Validate = Validate()

################################################################################
# Filters
#

    class Filters(object):
        '''Base class for filters commands.'''
        __slots__ = ()

        @debug_dump_args
        def run_filter(self, func, docs, req, userctx=None):
            if COUCHDB_VERSION < (0, 11, 1):
                filter_func = lambda doc: func(doc, req, userctx)
            else:
                filter_func = lambda doc: func(doc, req)
            return [True, [bool(filter_func(doc)) for doc in docs]]

        @debug_dump_args
        def run_filter_view(self, func, docs):
            return [True, [bool(tuple(func(doc))) for doc in docs]]

        def filter(self, *args):
            '''Used for filters design function set.

            Introduced in 0.10.0 version.
            Since 0.11.0 doesn't requires add_fun command proceeding before.
            Since 0.11.1 doesn't expects userctx argument any more.

            Command:
                filter

            Arguments:
                func: Function object.
                docs: List of documents each one of will be passed though filter.
                req: Request info dict.
                userctx: User info dict. Not used since 0.11.1 version.

            Returns:
                Two element list where first element is True and second is
                list of booleans which marks is document passed filter or not.

            Example of filter function:
                >>> def filterfun(doc, req):
                >>>     return doc['type'] == 'post'
            '''
            if COUCHDB_VERSION < (0, 11, 0):
                func = State.functions[0]
            else:
                func, args = args[0], args[1:]
            return self.run_filter(func, *args)

        def filter_view(self, *args):
            '''Used to apply filter function set to views.

            Introduced in 1.1.0 version.

            Command:
                views

            Arguments:
                func: Map function object.
                docs: List of documents.

            Returns:
                Two element list of True and list of booleans which marks is
                view generated result for passed document or not.

            Example would be same as view map function, just make call:
                GET /db/_changes?filter=_view&view=design_name/view_name
            '''
            return self.run_filter_view(*args)

    Filters = Filters()

################################################################################
# Render
#

    class Render(object):
        '''Base class for render commands. Used since 0.10.0 version.'''
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

        @debug_dump_args
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
                except ValueError, err:
                    log.exception('Error converting JSON to object: %s\n'
                                  'Reason: %s', line, err)
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

        @debug_dump_args
        def run_show(self, fun, *args):
            try:
                self.reset_list()
                Mime.reset_provides()
                resp = fun(*args) or {}
                if self.chunks:
                    resp = self.maybe_wrap_response(resp)
                    if not 'headers' in resp:
                        resp['headers'] = {}
                    resp['headers'].update(self.startresp)
                    resp['body'] = ''.join(self.chunks) + resp.get('body', '')
                    self.reset_list()
                if Mime.provides_used:
                    resp = Mime.run_provides(args[1])
                    resp = self.maybe_wrap_response(resp)
                    resp = self.apply_content_type(resp, Mime.resp_content_type)
                if isinstance(resp, (dict, basestring)):
                    return ['resp', self.maybe_wrap_response(resp)]
                else:
                    log.debug('resp: %r ; type: %r', resp, type(resp))
                    raise Error('render_error',
                                'undefined response from show function')
            except Exception, err:
                if args[0] is None and self.is_doc_request_path(args[1]):
                    raise Error('not_found', 'document not found')
                if isinstance(err, ViewServerException):
                    raise
                else:
                    log.exception('unexpected error occured')
                    raise Error('render_error', str(err))

        @debug_dump_args
        def run_update(self, fun, *args):
            try:
                method = args[1]['method']
                if method == 'GET':
                    log.debug('method: %s', method)
                    raise Error('method_not_allowed',
                                'update functions do not allow GET')
                doc, resp = fun(*args)
                if isinstance(resp, (dict, basestring)):
                    return ['up', doc, self.maybe_wrap_response(resp)]
                else:
                    log.debug('resp: %r ; type: %r', resp, type(resp))
                    raise Error('render_error',
                                'undefined response from update function')
            except ViewServerException:
                raise
            except Exception, err:
                log.exception('unexpected error occured')
                raise Error('render_error', str(err))

        @debug_dump_args
        def run_list(self, fun, *args):
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
            except Exception, err:
                log.exception('unexpected error occured')
                raise Error('render_error', str(err))

        def list(self, *args):
            '''Used for lists design function set.

            Introduced in 0.10.0 version.

            Command:
                list

            Arguments:
                func: Compiled function object. Used since 0.11.0.
                doc: Document object as dict.
                req: Request dict.
            '''
            if COUCHDB_VERSION < (0, 11, 0):
                func = State.functions[0]
            else:
                func, args = args[0], args[1:]
            return self.run_list(func, *args)

        def show(self, func, *args):
            '''Used for shows design function set.

            Introduced in 0.10.0 version.

            Command:
                show

            Arguments:
                func: Function source string.
                    Since 0.11.0 function object used instead of source string.
                doc: Document object as dict.
                req: Request info dict.

            Example of show function:
                >>> def show(doc, req):
                >>>     return {
                >>>         'code': 200,
                >>>         'headers': {
                >>>             'X-CouchDB-Python': '0.9.0'
                >>>         },
                >>>         'body': 'Hello, World!'
                >>>     }

            '''
            if COUCHDB_VERSION < (0, 11, 0):
                func = compile_func(func)
            return self.run_show(func, *args)

        def update(self, func, *args):
            '''Proceeds updates design function set.

            Introduced in 0.10.0 version.

            Command:
                update

            Arguments:
                func: Function source string.
                      Since 0.11.0 function object used instead of source string.
                doc: Document object as dict.
                req: Request info dict.

            Returns:
                Three element list: ["up", doc, response]
                If the doc is None no document will be committed to the
                database. If document an existing, it should already have an _id
                set. If it doesn't exists it will be created.
                Response object could be string or dict object, which
                will be returned to caller.

            Raises:
                Error: If request method was not POST/PUT.
                       If response is not dict object or basestring.

            Example of update function:
                >>> # http://wiki.apache.org/couchdb/Document_Update_Handlers
                >>> def update(doc, req):
                >>>     if not doc:
                >>>         if 'id' in req:
                >>>             # create new document
                >>>             return [{'_id': req['id']}, 'New World']
                >>>         # change nothing in database
                >>>         return [None, 'Empty World']
                >>>     doc['world'] = 'hello'
                >>>     doc['edited_by'] = req.get('userCtx')
                >>>     # update document in database
                >>>     return [doc, "Hello, doc!"]
                Update function must return two element set of doc and response.
            '''
            if COUCHDB_VERSION < (0, 11, 0):
                func = compile_func(func)
            return self.run_update(func, *args)

        def html_render_error(self, err, funstr):
            '''obsolete'''
            import cgi
            return {
                'body':''.join([
                '<html><body><h1>Render Error</h1>',
                str(err),
                '</p><h2>Stacktrace:</h2><code><pre>',
                cgi.escape(traceback.format_exc()),
                '</pre></code><h2>Function source:</h2><code><pre>',
                cgi.escape(funstr)])
            }

    Render = Render()

################################################################################
# Render used only for 0.9.x
#

    class RenderOld(object):
        '''Base class for render commands. Used only with 0.9.x versions.'''
        __slots__ = ()
        row_line = {}

        @debug_dump_args
        def render_function(self, func, args, funstr=None):
            try:
                resp = func(*args)
                if resp:
                    return Render.maybe_wrap_response(resp)
                else:
                    raise Error('render_error', 'undefined response from render'
                                                ' function: %s' % resp)
            except ViewServerException:
                raise
            except Exception, err:
                _log('function raised error: %s' % err)
                _log('stacktrace: %s' % traceback.format_exc())
                raise Error('render_error', str(err))

        @debug_dump_args
        def response_with(self, req, responders):
            best_mime = 'text/plain'
            best_key = None
            if 'headers' in req:
                accept = req['headers'].get('Accept')
            else:
                accept = None
            query = req.get('query', {})
            if accept is not None and 'format' not in query:
                provides = []
                for key in responders:
                    if key in Mime.mimes_by_key:
                        provides += list(Mime.mimes_by_key[key])
                best_mime = Mimeparse.best_match(provides, accept)
                best_key = Mime.keys_by_mime.get(best_mime)
            else:
                best_key = query.get('format')
            rfunc = responders.get(best_key or responders.get('fallback', 'html'))
            if rfunc is not None:
                resp = Render.maybe_wrap_response(rfunc())
                if not 'headers' in resp:
                    resp['headers'] = {}
                resp['headers']['Content-Type'] = best_mime
                return resp
            else:
                return {'code': 406, 'body': 'Not Acceptable: %s' % accept}

        @debug_dump_args
        def show_doc(self, funstr, doc, req=None):
            func = compile_func(funstr)
            return self.render_function(func, [doc, req])

        @debug_dump_args
        def list_begin(self, head, req):
            func = State.functions[0]
            self.row_line[func] = {
                'first_key': None,
                'row_number': 0,
                'prev_key': None
            }
            return self.render_function(func, [head, None, req, None])

        @debug_dump_args
        def list_row(self, row, req):
            func = State.functions[0]
            funstr = State.functions_src[0]
            row_info = self.row_line.get(func, None)
            assert row_info is not None
            resp = self.render_function(func, [None, row, req, row_info], funstr)
            if row_info['first_key'] is None:
                row_info['first_key'] = row.get('key')
            row_info['prev_key'] = row.get('key')
            row_info['row_number'] += 1
            self.row_line[func] = row_info
            return resp

        @debug_dump_args
        def list_tail(self, req):
            func = State.functions[0]
            row_info = self.row_line.pop(func, None)
            return self.render_function(func, [None, None, req, row_info])

    RenderOld = RenderOld()

################################################################################
# DDoc used since 0.11.0
#

    class DDoc(object):
        __slots__ = ()
        ddocs = {}

        def dispatcher(self):
            result = {
                'lists': Render.list,
                'shows': Render.show,
                'filters': Filters.filter,
                'updates': Render.update,
                'validate_doc_update': Validate.validate
            }
            if COUCHDB_VERSION == TRUNK:
                result['views'] = Filters.filter_view;
            return result

        @debug_dump_args
        def ddoc(self, *_args):
            '''Prepares proceeding of render/filter/validate functions.

            Also holds cache of design documents, but ddoc must have to be
            registered before proceeding.

            Command:
                ddoc

            Arguments:
                To put ddoc into cache:
                    "new": String contant, sign of this operation.
                    ddoc_id: Design document id.
                    ddoc: Design document itself.
                To call function from ddoc:
                    ddoc_id: Design document id, holder of requested function.
                    func_path: List of nodes by which request function could
                        be found. First element of this list is ddoc command.
                    func_args: List of function arguments.

            Returns:
                If ddoc putted into cache True will be returned.
                If ddoc function called returns it's result if any exists.
                For example, lists doesn't explicity returns any response.

            Raises:
                FatalError: If tried to work with uncached design document.
                            If unknown ddoc command passed.
            '''
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
                    prev, point = point, point[item]
                else:
                    func = point
                    if type(func) is not FunctionType:
                        func = compile_func(func, ddoc)
                        prev[item] = func
                return dispatch[cmd](func, *func_args)

    DDoc = DDoc()

################################################################################
# Context for function compilation
#

    def context():
        result = {
            'log': _log,
            'json': json,
            'Forbidden': Forbidden,
            'Error': Error,
            'FatalError': FatalError
        }
        if (0, 9, 0) <= COUCHDB_VERSION:
            result['provides'] = Mime.provides
            result['register_type'] = Mime.register_type
        if (0, 9, 0) <= COUCHDB_VERSION < (0, 10, 0):
            result['response_with'] = RenderOld.response_with
        elif COUCHDB_VERSION >= (0, 10, 0):
            result['start'] = Render.start
            result['send'] = Render.send
            result['get_row'] = Render.get_row
        return result

    context = context()

################################################################################
# Main loop handlers
#

    def handlers():
        result = {
            'reset': State.reset,
            'add_fun': State.add_fun,
            'map_doc': Views.map_doc,
            'reduce': Views.reduce,
            'rereduce': Views.rereduce
        }
        if (0, 9, 0) <= COUCHDB_VERSION < (0, 10, 0):
            result.update({
                'show_doc': RenderOld.show_doc,
                'list_begin': RenderOld.list_begin,
                'list_row': RenderOld.list_row,
                'list_tail': RenderOld.list_tail,
                'validate': Validate.validate
            })
        elif (0, 10, 0) <= COUCHDB_VERSION < (0, 11, 0):
            result.update({
                'list': Render.list,
                'show': Render.show,
                'filter': Filters.filter,
                'update': Render.update,
                'validate': Validate.validate
            })
        elif COUCHDB_VERSION >= (0, 11, 0):
            result['ddoc'] = DDoc.ddoc
        if (1, 1, 0) <= COUCHDB_VERSION:
            result['add_lib'] = State.add_lib
        return result

    handlers = handlers()

################################################################################
# Main loop itself
#

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
            except FatalError, err:
                log.exception('FatalError occured %s: %s', *err.args)
                log.critical('That was a critical error, exiting')
                respond(err.encode())
                return 1
            except Forbidden, err:
                reason = err.args[0]
                log.warn('ForbiddenError occured: %s', reason)
                respond(err.encode())
            except Error, err:
                log.exception('Error occured %s: %s', *err.args)
                respond(err.encode())
            except Exception, err:
                err_name = err.__class__.__name__
                err_msg = str(err)
                respond(Error(err_name, err_msg).encode())
                log.exception('%s: %s', err_name, err_msg)
                log.critical('That was a critical error, exiting')
                return 1
            else:
                log.debug('Returning  %r', retval)
                respond(retval)
    except KeyboardInterrupt:
        return 0
    except Exception, err:
        log.exception('Unexpected error occured: %s', err)
        log.critical('That was a critical error, exiting')
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
                          default: latest implemented.
                          Supports from 0.9.0 to 1.1.0 and trunk. Technicaly
                          should work with 0.8.0.
                          e.g.: --couchdb-version=0.9.0

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
        version = TRUNK
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
                if value.lower() != 'trunk':
                    version = value.split('.')
                    while len(version) < 3:
                        version.append(0)
                    _run_version = '.'.join(version[:3])
                    version = tuple(map(int, version[:3]))
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
    sys.exit(run(version=version))


if __name__ == '__main__':
    main()
