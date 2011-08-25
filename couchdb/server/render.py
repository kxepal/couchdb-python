# -*- coding: utf-8 -*-
#
import logging
from types import FunctionType
from couchdb.server import mime
from couchdb.server.exceptions import Error, FatalError, ViewServerException
from couchdb.server.helpers import partial

__all__ = ['show', 'list', 'update',
           'show_doc', 'list_begin', 'list_row', 'list_tail',
           'ChunkedReponder']

log = logging.getLogger(__name__)

class ChunkedReponder(object):

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
        self.startresp.clear()
        del self.chunks[:]

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
        self.startresp.clear()
        self.startresp.update(resp or {})
        del self.chunks[:]

    def send_start(self, resp_content_type):
        log.debug('Starting respond')
        resp = apply_content_type(self.startresp or {}, resp_content_type)
        self.write(['start', self.chunks, resp])
        del self.chunks[:]
        self.startresp.clear()

    def send(self, chunk):
        """Sends an HTTP chunk to the client.

        :param chunk: Response chunk object. Would be converted to unicode string.
        :type chunk: unicode or utf-8 encoded string preferred.
        """
        self.chunks.append(unicode(chunk))

    def blow_chunks(self, label='chunks'):
        log.debug('Sending chunks')
        self.write([label, self.chunks])
        del self.chunks[:]

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
    try:
        mime_provider = mime.MimeProvider()
        responder = ChunkedReponder(
            server.receive, server.respond, mime_provider)
        func = apply_context(
            func,
            register_type = mime_provider.register_type,
            provides = mime_provider.provides,
            start = responder.start,
            send = responder.send,
            get_row = responder.get_row
        )
        resp = func(doc, req) or {}
        if responder.chunks:
            resp = maybe_wrap_response(resp)
            if not 'headers' in resp:
                resp['headers'] = {}
            for key, value in responder.startresp.items():
                assert isinstance(key, basestring), 'invalid header name'
                assert isinstance(value, basestring), 'invalid header value'
                resp['headers'][key] = value
            resp['body'] = ''.join(responder.chunks) + resp.get('body', '')
            responder.reset()
        if mime_provider.is_provides_used():
            resp = mime_provider.run_provides(req)
            resp = maybe_wrap_response(resp)
            resp = apply_content_type(resp, mime_provider.resp_content_type)
        if not isinstance(resp, (dict, basestring)):
            msg = 'Invalid response object %r ; type: %r' % (resp, type(resp))
            log.error(msg)
            raise Error('render_error', msg)
        return ['resp', maybe_wrap_response(resp)]
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Unexpected exception occurred')
        if doc is None and is_doc_request_path(req):
            raise Error('not_found', 'document not found')
        raise Error('render_error', str(err))

def run_update(server, func, doc, req):
    try:
        method = req.get('method', None)
        if not server.config.get('allow_get_update', False) and method == 'GET':
            msg = 'Method `GET` is not allowed for update functions'
            log.error(msg)
            raise Error('method_not_allowed', msg)
        doc, resp = func(doc, req)
        if isinstance(resp, (dict, basestring)):
            return ['up', doc, maybe_wrap_response(resp)]
        else:
            msg = 'Invalid response object %r ; type: %r' % (resp, type(resp))
            log.error(msg)
            raise Error('render_error', msg)
    except ViewServerException:
        raise
    except Exception, err:
        log.exception('Unexpected exception occurred')
        raise Error('render_error', str(err))

def run_list(server, func, head, req):
    try:
        mime_provider = mime.MimeProvider()
        responder = ChunkedReponder(
            server.receive, server.respond, mime_provider)
        func = apply_context(
            func,
            register_type = mime_provider.register_type,
            provides = mime_provider.provides,
            start = responder.start,
            send = responder.send,
            get_row = responder.get_row
        )
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
        log.exception('Unexpected exception occurred')
        raise Error('render_error', str(err))

def list(server, head, req):
    """Implementation of `list` command. Should be prequested by ``add_fun``
    command.

    :command: list

    :param head: View result information.
    :param req: Request info.
    :type head: dict
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

    :param func: List function object.
    :param head: View result information.
    :param req: Request info.
    :type func: function
    :type head: dict
    :type req: dict

    .. versionadded:: 0.11.0
    """
    return run_list(server, func, head, req)

def show(server, func, doc, req):
    """Implementation of `show` command.

    :command: show

    :param func: Show function source.
    :param doc: Document object.
    :param req: Request info.
    :type func: unicode
    :type doc: dict
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

    :param func: Show function object.
    :param doc: Document object.
    :param req: Request info.
    :type func: function
    :type doc: dict
    :type req: dict

    .. versionadded:: 0.11.0
    """
    return run_show(server, func, doc, req)

def update(server, funsrc, doc, req):
    """Implementation of `update` command.

    :command: update

    :param funsrc: Update function source.
    :param doc: Document object.
    :param req: Request info.
    :type funsrc: unicode
    :type doc: dict
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

    :param func: Update function object.
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

    .. versionadded:: 0.11.0
    """
    return run_update(server, func, doc, req)


################################################################################
# Old render used only for 0.9.x
#

def render_function(func, args):
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
        log.exception('Unexpected exception occurred')
        raise Error('render_error', str(err))

def response_with(req, responders, mime_provider):
    """Context dispatcher method.

    :param req: Request info.
    :param responders: Handlers mapping to mime format.
    :type req: dict
    :type responders: dict

    :return: Response object.
    :rtype: dict
    """
    fallback = responders.pop('fallback', None)
    for key, func in responders.items():
        mime_provider.provides(key, func)
    try:
        resp = maybe_wrap_response(mime_provider.run_provides(req, fallback))
    except Error, err:
        mimetype = req.get('headers', {}).get('Accept')
        mimetype = req.get('query', {}).get('format', mimetype)
        return {'code': 406, 'body': 'Not Acceptable: %s' % mimetype}
    else:
        if not 'headers' in resp:
            resp['headers'] = {}
        resp['headers']['Content-Type'] = mime_provider.resp_content_type
        return resp

def show_doc(server, funsrc, doc, req):
    """Implementation of `show_doc` command.

    :command: show_doc

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
    return render_function(func, [doc, req])

def list_begin(server, head, req):
    """Initiates list rows generation.

    :command: list_begin

    :param head: Headers information.
    :param req: Request information.
    :type head: dict
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
    func = apply_context(func, response_with=response_with)
    return render_function(func, [head, None, req, None])

def list_row(server, row, req):
    """Generates single list row.

    :command: list_row

    :param row: View result information.
    :param req: Request information.
    :type row: dict
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    """
    func = server.state['functions'][0]
    row_info = server.state['row_line'].get(func, None)
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

    :param req: Request information.
    :type req: dict

    :return: Response object.
    :rtype: dict

    .. versionadded:: 0.9.0
    .. deprecated:: 0.10.0 Use :func:`list` instead.
    """
    func = server.state['functions'][0]
    row_info = server.state['row_line'].pop(func, None)
    func = apply_context(func, response_with=response_with)
    return render_function(func, [None, None, req, row_info])
