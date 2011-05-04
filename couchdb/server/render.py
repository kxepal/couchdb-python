# -*- coding: utf-8 -*-
#
import logging
import traceback
from couchdb.server import mime
from couchdb.server import state
from couchdb.server import stream
from couchdb.server.compiler import compile_func
from couchdb.server.exceptions import Error, FatalError, ViewServerException

__all__ = ['show', 'list', 'update',
           'show_doc', 'list_begin', 'list_row', 'list_tail',
           'response_with', 'start', 'send', 'get_row']

log = logging.getLogger(__name__)

chunks = []
startresp = {}
gotrow = False
lastrow = False

def reset_list():
    global gotrow, lastrow
    del chunks[:]
    startresp.clear()
    gotrow = False
    lastrow = False

def start(resp=None):
    '''Initiate HTTP response.

    :param resp: Initial response. Optional.
    :type resp: dict
    '''
    startresp.clear()
    startresp.update(resp or {})

def send_start():
    log.debug('Starting respond')
    resp = apply_content_type(startresp or {}, mime.resp_content_type)
    stream.respond(['start', chunks, resp])
    del chunks[:]
    startresp.clear()

def apply_content_type(resp, resp_content_type):
    if not resp.get('headers'):
        resp['headers'] = {}
    if resp_content_type and not resp['headers'].get('Content-Type'):
        resp['headers']['Content-Type'] = resp_content_type
    return resp

def send(chunk):
    '''Sends an HTTP chunk to the client.

    :param chunk: Response chunk object. Would be converted to unicode string.
    :type chunk: unicode or utf-8 encoded string prefered.
    '''
    chunks.append(unicode(chunk))

def blow_chunks(label='chunks'):
    log.debug('Sending chunks')
    stream.respond([label, chunks])
    del chunks[:]

def get_row():
    '''Yields a next row of view result.'''
    global gotrow, lastrow
    while True:
        if lastrow:
            break
        if not gotrow:
            gotrow = True
            send_start()
        else:
            blow_chunks()
        try:
            data = stream.receive().next()
        except StopIteration:
            break
        if data[0] == 'list_end':
            lastrow = True
            break
        if data[0] != 'list_row':
            log.error('Not a row `%s`' % data[0])
            raise FatalError('list_error', 'not a row `%s`' % data[0])
        yield data[1]

def maybe_wrap_response(resp):
    if isinstance(resp, basestring):
        return {'body': resp}
    else:
        return resp

def is_doc_request_path(info):
    return len(info['path']) > 5

def run_show(func, doc, req):
    try:
        reset_list()
        mime.reset_provides()
        resp = func(doc, req) or {}
        if chunks:
            resp = maybe_wrap_response(resp)
            if not 'headers' in resp:
                resp['headers'] = {}
            resp['headers'].update(startresp)
            resp['body'] = ''.join(chunks) + resp.get('body', '')
            reset_list()
        if mime.provides_used():
            resp = mime.run_provides(req)
            resp = maybe_wrap_response(resp)
            resp = apply_content_type(resp, mime.resp_content_type)
        if isinstance(resp, (dict, basestring)):
            return ['resp', maybe_wrap_response(resp)]
        else:
            log.error('Invalid response object %r ; type: %r', resp, type(resp))
            raise Error('render_error',
                        'undefined response from show function')
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Unexpected exception occured')
        if doc is None and is_doc_request_path(req):
            raise Error('not_found', 'document not found')
        raise Error('render_error', str(err))

def run_update(func, doc, req):
    try:
        method = req['method']
        if method == 'GET':
            log.error('update functions do not allow GET')
            raise Error('method_not_allowed',
                        'update functions do not allow GET')
        doc, resp = func(doc, req)
        if isinstance(resp, (dict, basestring)):
            return ['up', doc, maybe_wrap_response(resp)]
        else:
            log.error('Invalid response object %r ; type: %r', resp, type(resp))
            raise Error('render_error',
                        'undefined response from update function')
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Unexpected exception occured')
        raise Error('render_error', str(err))

def run_list(func, head, req):
    try:
        mime.reset_provides()
        reset_list()
        tail = func(head, req)
        if mime.provides_used():
            tail = mime.run_provides(req)
        if not gotrow:
            for row in get_row():
                break
        if tail is not None:
            chunks.append(tail)
        blow_chunks('end')
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Unexpected exception occured')
        raise Error('render_error', str(err))

def list(*args):
    '''Implemention of `list` / ddoc `lists` commands.

    :command: list / lists

    :param func: List function object.
    :param head: View result information.
    :param req: Request info.
    :type func: function
    :type doc: dict
    :type req: dict

    .. versionadded:: 0.10.0
    .. versionchanged:: 0.11.0 Now is a subcommand of :ref:`ddoc` as  `lists`.
    .. versionchanged:: 0.11.0 Add first parameter ``func`` as function object.
    '''
    if state.version < (0, 11, 0):
        func = state.functions[0]
    else:
        func, args = args[0], args[1:]
    return run_list(func, *args)

def show(func, doc, req):
    '''Implemention of `show` / ddoc `shows` commands.

    :command: show / shows

    :param func: Update function.
    :param doc: Document object.
    :param req: Request info.
    :type func: function
    :type doc: dict
    :type req: dict

    .. versionadded:: 0.10.0
    .. versionchanged:: 0.11.0 Now is a subcommand of :ref:`ddoc` as `shows`.
    .. versionchanged:: 0.11.0 ``func`` parameter have passed as function
            object instead of source string.
    '''
    if state.version < (0, 11, 0):
        func = compile_func(func)
    return run_show(func, doc, req)

def update(func, doc, req):
    '''Implemention of `update` / ddoc `updates` commands.

    :command: update / updates

    :param func: Update function.
    :param doc: Document object.
    :param req: Request info.
    :type func: function
    :type doc: dict
    :type req: dict

    :return: Three element list: ["up", doc, response]
    :rtype: list

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If request method was GET.
          If response was not dict object or basestring.

    .. versionadded:: 0.10.0
    .. versionchanged:: 0.11.0 Now is a subcommand of :ref:`ddoc` as `updates`.
    .. versionchanged:: 0.11.0 ``func`` parameter have passed as function
            object instead of source string.
    '''
    if state.version < (0, 11, 0):
        func = compile_func(func)
    return run_update(func, doc, req)

def html_render_error(err, funstr):
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


################################################################################
# Old render used only for 0.9.x
#

row_line = {}

def render_function(func, args, funstr=None):
    try:
        resp = func(*args)
        if resp:
            return maybe_wrap_response(resp)
        else:
            log.error('undefined response from render')
            raise Error('render_error', 'undefined response from render'
                                        ' function: %s' % resp)
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Unexpected exception occured')
        raise Error('render_error', str(err))

def response_with(req, responders):
    '''Context dispatcher method.

    :param req: Request info.
    :param responders: Handlers mapping to mime format.
    :type req: dict
    :type responders: dict

    :return: Response object.
    :rtype: dict
    '''
    best_mime = 'text/plain'
    best_key = None
    accept = 'headers' in req and req['headers'].get('Accept') or None
    query = req.get('query', {})
    if accept is not None and 'format' not in query:
        provides = [item for key in responders
                         for item in mime.mimes_by_key.get(key, ())]
        best_mime = mime.best_match(provides, accept)
        best_key = mime.keys_by_mime.get(best_mime)
    else:
        best_key = query.get('format')
    rfunc = responders.get(best_key or responders.get('fallback', 'html'))
    if rfunc is not None:
        resp = maybe_wrap_response(rfunc())
        if not 'headers' in resp:
            resp['headers'] = {}
        resp['headers']['Content-Type'] = best_mime
        return resp
    else:
        return {'code': 406, 'body': 'Not Acceptable: %s' % accept}

def show_doc(funstr, doc, req=None):
    '''Implemention of `show_doc` command.

    :command: show_doc

    :param funstr: Python function source code.
    :param doc: Document object.
    :param req: Request info.
    :type func: basestring
    :type doc: dict
    :type req: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`show` instead.
    '''
    func = compile_func(funstr)
    return render_function(func, [doc, req])

def list_begin(head, req):
    '''Initiates list rows generation.

    :command: list_begin

    :param head: Headers information.
    :param req: Request information.
    :type head: dict
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    '''
    func = state.functions[0]
    row_line[func] = {
        'first_key': None,
        'row_number': 0,
        'prev_key': None
    }
    return render_function(func, [head, None, req, None])

def list_row(row, req):
    '''Generates single list row.

    :command: list_row

    :param row: View result information.
    :param req: Request information.
    :type row: dict
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    '''
    func = state.functions[0]
    funstr = state.functions_src[0]
    row_info = row_line.get(func, None)
    assert row_info is not None
    resp = render_function(func, [None, row, req, row_info], funstr)
    if row_info['first_key'] is None:
        row_info['first_key'] = row.get('key')
    row_info['prev_key'] = row.get('key')
    row_info['row_number'] += 1
    row_line[func] = row_info
    return resp

def list_tail(req):
    '''Finishes list result output.

    :command: list_tail

    :param req: Request information.
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    '''
    func = state.functions[0]
    row_info = row_line.pop(func, None)
    return render_function(func, [None, None, req, row_info])
