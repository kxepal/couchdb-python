# -*- coding: utf-8 -*-
#
import logging

__all__ = ['filter', 'ddoc_filter', 'ddoc_views']

log = logging.getLogger(__name__)

def run_filter(func, docs, *args):
    return [True, [bool(func(doc, *args)) for doc in docs]]

def run_filter_view(func, docs):
    result = []
    for doc in docs:
        for item in func(doc):
            result.append(True)
            break
        else:
            result.append(False)
    return [True, result]

def filter(server, docs, req, userctx=None):
    """Implementation of `filter` command. Should be preceded  by ``add_fun``
    command.

    :command: filter

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param docs: List of documents each one of will be passed though filter.
    :type docs: list

    :param req: Request info.
    :type req: dict

    :param userctx: User info.
    :type userctx: dict

    :return:
        Two element list where first element is True and second is list of
        booleans per document which marks has document passed filter or not.
    :rtype: list

    .. versionadded:: 0.10.0
    .. deprecated:: 0.11.0
        Now is a subcommand of :ref:`ddoc`.
        Use :func:`~couchdb.server.filters.ddoc_filter` instead.
    """
    return run_filter(server.state['functions'][0], docs, req, userctx)

def ddoc_filter(server, func, docs, req, userctx=None):
    """Implementation of ddoc `filters` command.

    :command: filters

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param func: Filter function object.
    :type func: function

    :param docs: List of documents each one of will be passed though filter.
    :type docs: list

    :param req: Request info.
    :type req: dict

    :param userctx: User info.
    :type userctx: dict

    :return:
        Two element list where first element is True and second is list of
        booleans per document which marks has document passed filter or not.
    :rtype: list

    .. versionadded:: 0.11.0
    .. versionchanged:: 0.11.1
        Removed ``userctx`` argument. Use ``req['userctx']`` instead.
    """
    if server.version < (0, 11, 1):
        args = req, userctx
    else:
        args = req,
    return run_filter(func, docs, *args)

def ddoc_views(server, func, docs):
    """Implementation of ddoc `views` command. Filters ``_changes`` feed using
    view map function.

    :command: views

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param func: Map function object.
    :type func: function

    :param docs: List of documents.
    :type docs: list

    :return: Two element list of True and list of booleans which marks is
        view generated result for passed document or not.

    Example would be same as view map function, just make call::

        GET /db/_changes?filter=_view&view=design_name/view_name

    .. versionadded:: 1.1.0
    """
    return run_filter_view(func, docs)
