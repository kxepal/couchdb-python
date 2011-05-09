# -*- coding: utf-8 -*-
#
import logging
from couchdb.server import state

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

def filter(docs, req, userctx=None):
    '''Implementation of `filter` command. Should be prequested by ``add_fun``
    command.

    :command: filter

    :param docs: List of documents each one of will be passed though filter.
    :param req: Request info.
    :param userctx: User info.
    :type docs: list
    :type req: dict
    :type userctx: dict

    :return:
        Two element list where first element is True and second is list of
        booleans per document which marks has document passed filter or not.
    :rtype: list

    .. versionadded:: 0.10.0
    .. deprecated:: 0.11.0
        Now is a subcommand of :ref:`ddoc`.
        Use :func:`~couchdb.server.filters.ddoc_filter` instead.
    '''
    return run_filter(state.functions[0], docs, req, userctx)

def ddoc_filter(func, docs, req, userctx=None):
    '''Implementation of ddoc `filters` command.

    :command: filters

    :param func: Filter function object.
    :param docs: List of documents each one of will be passed though filter.
    :param req: Request info.
    :param userctx: User info.
    :type func: function
    :type docs: list
    :type req: dict
    :type userctx: dict

    :return:
        Two element list where first element is True and second is list of
        booleans per document which marks has document passed filter or not.
    :rtype: list

    .. versionadded:: 0.11.0
    .. versionchanged:: 0.11.1
        Removed ``userctx`` argument. Use ``req['userctx']`` instead.
    '''
    if state.version < (0, 11, 1):
        args = req, userctx
    else:
        args = req,
    return run_filter(func, docs, *args)

def ddoc_views(func, docs):
    '''Implementation of ddoc `views` command. Filters ``_changes`` feed using
    view map function.

    :command: views

    :param func: Map function object.
    :param docs: List of documents.

    :return: Two element list of True and list of booleans which marks is
        view generated result for passed document or not.

    Example would be same as view map function, just make call::

        GET /db/_changes?filter=_view&view=design_name/view_name

    .. versionadded:: 1.1.0
    '''
    return run_filter_view(func, docs)
