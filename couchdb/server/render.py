# -*- coding: utf-8 -*-
#
import logging
from types import FunctionType
from couchdb.server import mime
from couchdb.server.exceptions import Error, FatalError, ViewServerException
from couchdb.server.helpers import partial

__all__ = ['show', 'list', 'update',
           'show_doc', 'list_begin', 'list_row', 'list_tail',
           'ChunkedResponder']

log = logging.getLogger(__name__)

class ChunkedResponder(object):

    def __init__(self, input, output, mime_provider):
        self.gotrow = False
        self.lastrow = False
        self.startresp = {}
        self.chunks = []
        self.read = input
        self.write = output
        self.mime_provider = mime_provider

    def reset(self):
        self.gotrow = False
        self.lastrow = False
        self.startresp = {}
        self.chunks = []

    def get_row(self):
        """Yields a next row of view result."""
        reader = self.read()
        while True:
            if self.lastrow:
                break
            if not self.gotrow:
                self.gotrow = True
                self.send_start(self.mime_provider.resp_content_type)
            else:
                self.blow_chunks()
            try:
                data = reader.next()
            except StopIteration:
                break
            if data[0] == 'list_end':
                self.lastrow = True
                break
            if data[0] != 'list_row':
                log.error('Not a row `%s`' % data[0])
                raise FatalError('list_error', 'not a row `%s`' % data[0])
            yield data[1]

    def start(self, resp=None):
        """Initiate HTTP response.

        :param resp: Initial response. Optional.
        :type resp: dict
        """
        self.startresp = resp or {}
        self.chunks = []

    def send_start(self, resp_content_type):
        log.debug('Start response with %s content type', resp_content_type)
        resp = apply_content_type(self.startresp or {}, resp_content_type)
        self.write(['start', self.chunks, resp])
        self.chunks = []
        self.startresp = {}

    def send(self, chunk):
        """Sends an HTTP chunk to the client.

        :param chunk: Response chunk object.
                      Would be converted to unicode string.
        :type chunk: unicode or utf-8 encoded string preferred.
        """
        if not isinstance(chunk, unicode):
            chunk = unicode(chunk, 'utf-8')
        self.chunks.append(chunk)

    def blow_chunks(self, label='chunks'):
        log.debug('Send chunks')
        self.write([label, self.chunks])
        self.chunks = []

def apply_context(func, **context):
    func.func_globals.update(context)
    func = FunctionType(func.func_code, func.func_globals)
    return func

def apply_content_type(resp, resp_content_type):
    if not resp.get('headers'):
        resp['headers'] = {}
    if resp_content_type and not resp['headers'].get('Content-Type'):
        resp['headers']['Content-Type'] = resp_content_type
    return resp

def maybe_wrap_response(resp):
    if isinstance(resp, basestring):
        return {'body': resp}
    else:
        return resp

def is_doc_request_path(info):
    return len(info['path']) > 5

def run_show(server, func, doc, req):
    log.debug('Run show %s\ndoc: %s\nreq: %s', func, doc, req)
    mime_provider = mime.MimeProvider()
    responder = ChunkedResponder(server.receive, server.respond, mime_provider)
    func = apply_context(
        func,
        register_type = mime_provider.register_type,
        provides = mime_provider.provides,
        start = responder.start,
        send = responder.send
    )
    try:
        resp = func(doc, req) or {}
        if responder.chunks:
            resp = maybe_wrap_response(resp)
            if not 'headers' in resp:
                resp['headers'] = {}
            for key, value in responder.startresp.items():
                assert isinstance(key, str), 'invalid header key %r' % key
                assert isinstance(value, str), 'invalid header value %r' % value
                resp['headers'][key] = value
            resp['body'] = ''.join(responder.chunks) + resp.get('body', '')
            responder.reset()
        if mime_provider.is_provides_used():
            provided_resp = mime_provider.run_provides(req) or {}
            provided_resp = maybe_wrap_response(provided_resp)
            body = provided_resp.get('body', '')
            if responder.chunks:
                body = resp.get('body', '') + ''.join(responder.chunks)
                body += provided_resp.get('body', '')
            resp.update(provided_resp)
            if 'body' in resp:
                resp['body'] = body
            resp = apply_content_type(resp, mime_provider.resp_content_type)
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Show %s raised an error:\n'
                      'doc: %s\nreq: %s\n', func, doc, req)
        if doc is None and is_doc_request_path(req):
            raise Error('not_found', 'document not found')
        raise Error('render_error', str(err))
    else:
        resp = maybe_wrap_response(resp)
        log.debug('Show %s response\n%s', func, resp)
        if not isinstance(resp, (dict, basestring)):
            msg = 'Invalid response object %r ; type: %r' % (resp, type(resp))
            log.error(msg)
            raise Error('render_error', msg)
        return ['resp', resp]

def run_update(server, func, doc, req):
    log.debug('Run update %s\ndoc: %s\nreq: %s', func, doc, req)
    method = req.get('method', None)
    if not server.config.get('allow_get_update', False) and method == 'GET':
        msg = 'Method `GET` is not allowed for update functions'
        log.error(msg + '.\nRequest: %s', req)
        raise Error('method_not_allowed', msg)
    try:
        doc, resp = func(doc, req)
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Update %s raised an error:\n'
                      'doc: %s\nreq: %s\n', func, doc, req)
        raise Error('render_error', str(err))
    else:
        resp = maybe_wrap_response(resp)
        log.debug('Update %s response\n%s', func, resp)
        if isinstance(resp, (dict, basestring)):
            return ['up', doc, resp]
        else:
            msg = 'Invalid response object %r ; type: %r' % (resp, type(resp))
            log.error(msg)
            raise Error('render_error', msg)

def run_list(server, func, head, req):
    log.debug('Run list %s\nhead: %s\nreq: %s', func, head, req)
    mime_provider = mime.MimeProvider()
    responder = ChunkedResponder(server.receive, server.respond, mime_provider)
    func = apply_context(
        func,
        register_type = mime_provider.register_type,
        provides = mime_provider.provides,
        start = responder.start,
        send = responder.send,
        get_row = responder.get_row
    )
    try:
        tail = func(head, req)
        if mime_provider.is_provides_used():
            tail = mime_provider.run_provides(req)
        if not responder.gotrow:
            for row in responder.get_row():
                break
        if tail is not None:
            responder.send(tail)
        responder.blow_chunks('end')
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('List %s raised an error:\n'
                      'head: %s\nreq: %s\n', func, head, req)
        raise Error('render_error', str(err))

def list(server, head, req):
    """Implementation of `list` command. Should be prequested by ``add_fun``
    command.

    :command: list

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param head: View result information.
    :type head: dict

    :param req: Request info.
    :type req: dict

    .. versionadded:: 0.10.0
    .. deprecated:: 0.11.0
        Now is a subcommand of :ref:`ddoc`.
        Use :func:`~couchdb.server.render.ddoc_list` instead.
    """
    func = server.state['functions'][0]
    return run_list(server, func, head, req)

def ddoc_list(server, func, head, req):
    """Implementation of ddoc `lists` command.

    :command: lists

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param func: List function object.
    :type func: function

    :param head: View result information.
    :type head: dict

    :param req: Request info.
    :type req: dict

    .. versionadded:: 0.11.0
    """
    return run_list(server, func, head, req)

def show(server, func, doc, req):
    """Implementation of `show` command.

    :command: show

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param func: Show function source.
    :type func: unicode

    :param doc: Document object.
    :type doc: dict

    :param req: Request info.
    :type req: dict

    .. versionadded:: 0.10.0
    .. deprecated:: 0.11.0
        Now is a subcommand of :ref:`ddoc`.
        Use :func:`~couchdb.server.render.ddoc_show` instead.
    """
    return run_show(server, server.compile(func), doc, req)

def ddoc_show(server, func, doc, req):
    """Implementation of ddoc `shows` command.

    :command: shows

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param func: Show function object.
    :type func: function

    :param doc: Document object.
    :type doc: dict

    :param req: Request info.
    :type req: dict

    .. versionadded:: 0.11.0
    """
    return run_show(server, func, doc, req)

def update(server, funsrc, doc, req):
    """Implementation of `update` command.

    :command: update

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param funsrc: Update function source.
    :type funsrc: unicode

    :param doc: Document object.
    :type doc: dict

    :param req: Request info.
    :type req: dict

    :return: Three element list: ["up", doc, response]
    :rtype: list

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If request method was GET.
          If response was not dict object or basestring.

    .. versionadded:: 0.10.0
    .. deprecated:: 0.11.0
        Now is a subcommand of :ref:`ddoc`.
        Use :func:`~couchdb.server.render.ddoc_update` instead.
    """
    return run_update(server, server.compile(funsrc), doc, req)

def ddoc_update(server, func, doc, req):
    """Implementation of ddoc `updates` commands.

    :command: updates

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param func: Update function object.
    :type func: function

    :param doc: Document object.
    :type doc: dict

    :param req: Request info.
    :type req: dict

    :return: Three element list: ["up", doc, response]
    :rtype: list

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If request method was GET.
          If response was not dict object or basestring.

    .. versionadded:: 0.11.0
    """
    return run_update(server, func, doc, req)


################################################################################
# Old render used only for 0.9.x
#

def render_function(func, args):
    try:
        resp = maybe_wrap_response(func(*args))
        if isinstance(resp, (dict, basestring)):
            return resp
        else:
            msg = 'Invalid response object %r ; type: %r' % (resp, type(resp))
            log.error(msg)
            raise Error('render_error', msg)
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Unexpected exception occurred in %s', func)
        raise Error('render_error', str(err))

def response_with(req, responders, mime_provider):
    """Context dispatcher method.

    :param req: Request info.
    :type req: dict

    :param responders: Handlers mapping to mime format.
    :type responders: dict

    :param mime_provider: Mime provider instance.
    :type mime_provider: :class:`~couchdb.server.mime.MimeProvider`

    :return: Response object.
    :rtype: dict
    """
    fallback = responders.pop('fallback', None)
    for key, func in responders.items():
        mime_provider.provides(key, func)
    try:
        resp = maybe_wrap_response(mime_provider.run_provides(req, fallback))
    except Error, err:
        if err.args[0] != 'not_acceptable':
            log.exception('Unexpected error raised:\n'
                          'req: %s\nresponders: %s', req, responders)
            raise
        mimetype = req.get('headers', {}).get('Accept')
        mimetype = req.get('query', {}).get('format', mimetype)
        log.warn('Not acceptable content-type: %s', mimetype)
        return {'code': 406, 'body': 'Not acceptable: %s' % mimetype}
    else:
        if not 'headers' in resp:
            resp['headers'] = {}
        resp['headers']['Content-Type'] = mime_provider.resp_content_type
        return resp

def show_doc(server, funsrc, doc, req):
    """Implementation of `show_doc` command.

    :command: show_doc

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param funsrc: Python function source code.
    :type funsrc: basestring

    :param doc: Document object.
    :type doc: dict

    :param req: Request info.
    :type req: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`show` instead.
    """
    mime_provider = mime.MimeProvider()
    context = {
        'response_with': partial(response_with, mime_provider=mime_provider),
        'register_type': mime_provider.register_type
    }
    func = server.compile(funsrc, context=context)
    log.debug('Run show %s\ndoc: %s\nreq: %s\nfunsrc:\n%s',
              func, doc, req, funsrc)
    return render_function(func, [doc, req])

def list_begin(server, head, req):
    """Initiates list rows generation.

    :command: list_begin

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param head: Headers information.
    :type head: dict

    :param req: Request information.
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    """
    func = server.state['functions'][0]
    server.state['row_line'][func] = {
        'first_key': None,
        'row_number': 0,
        'prev_key': None
    }
    log.debug('Run list begin %s\nhead: %s\nreq: %s', func, head, req)
    func = apply_context(func, response_with=response_with)
    return render_function(func, [head, None, req, None])

def list_row(server, row, req):
    """Generates single list row.

    :command: list_row

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param row: View result information.
    :type row: dict

    :param req: Request information.
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    """
    func = server.state['functions'][0]
    row_info = server.state['row_line'].get(func, None)
    log.debug('Run list row %s\nrow: %s\nreq: %s', func, row, req)
    func = apply_context(func, response_with=response_with)
    assert row_info is not None
    resp = render_function(func, [None, row, req, row_info])
    if row_info['first_key'] is None:
        row_info['first_key'] = row.get('key')
    row_info['prev_key'] = row.get('key')
    row_info['row_number'] += 1
    server.state['row_line'][func] = row_info
    return resp

def list_tail(server, req):
    """Finishes list result output.

    :command: list_tail

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param req: Request information.
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    """
    func = server.state['functions'][0]
    row_info = server.state['row_line'].pop(func, None)
    log.debug('Run list row %s\nrow_info: %s\nreq: %s', func, row_info, req)
    func = apply_context(func, response_with=response_with)
    return render_function(func, [None, None, req, row_info])
