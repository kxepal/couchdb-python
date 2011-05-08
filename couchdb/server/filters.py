# -*- coding: utf-8 -*-
#
import logging
from couchdb.server import state

__all__ = ['filter', 'filter_view']

log = logging.getLogger(__name__)

def run_filter(func, docs, req, userctx=None):
    if state.version < (0, 11, 1):
        args = req, userctx
    else:
        args = req,
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

def filter(*args):
    '''Implemention of `filter` / ddoc `filters` commands.

    :command: filter / filters

    :param func: Filter function object. Added since 0.11.0 version.
    :param docs: List of documents each one of will be passed though filter.
    :param req: Request info.
    :param userctx: User info. Not used since 0.11.1 version.
    :type func: function
    :type docs: list
    :type req: dict
    :type userctx: dict

    :return: Two element list where first element is True and second is
        list of booleans which marks is document passed filter or not.
    :rtype: list

    .. versionadded:: 0.10.0
    .. versionchanged:: 0.11.0 Added ``func`` argument as first.
    .. versionchanged:: 0.11.0 Now is a subcommand of :ref:`ddoc` as `filters`.
    .. versionchanged:: 0.11.1 Removed 4th argument ``userctx``.
                               Use ``req['userctx']`` instead.
    '''
    if state.version < (0, 11, 0):
        func = state.functions[0]
    else:
        func, args = args[0], args[1:]
    return run_filter(func, *args)

def filter_view(func, docs):
    '''Implemention of ddoc `views` commands.

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
