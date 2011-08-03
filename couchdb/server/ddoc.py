# -*- coding: utf-8 -*-
#
import logging
from types import FunctionType
from couchdb.server.compiler import compile_func
from couchdb.server.exceptions import FatalError, Error

__all__ = ['ddoc', 'commands']

log = logging.getLogger(__name__)

ddocs = {}
commands = {}

def ddoc(*args):
    """Prepares proceeding of :ref:`render` / :ref:`filters` / :ref:`validate`
    functions.

    Also holds cache of design documents, but ddoc must have to be
    registered before proceeding.

    :command: ddoc

    To put ddoc into cache:

    :param new: String constant "new".
    :param ddoc_id: Design document id.
    :param ddoc: Design document itself.
    :type new: unicode
    :type ddoc_id: unicode
    :type ddoc: dict
    :return: True

    To call function from ddoc:

    :param ddoc_id: Design document id, holder of requested function.
    :param func_path: List of nodes by which request function could be found.
        First element of this list is ddoc command.
    :param func_args: List of function arguments.
    :type ddoc_id: unicode
    :type func_path: list
    :type func_args: list

    :return: If ddoc putted into cache True will be returned.
        If ddoc function called returns it's result if any exists.
        For example, lists doesn't explicitly returns any value.

    .. versionadded:: 0.11.0
    .. versionchanged:: 1.1.0 Support for views subcommand.
    """
    args = list(args)
    ddoc_id = args.pop(0)
    if ddoc_id == 'new':
        ddoc_id = args.pop(0)
        ddocs[ddoc_id] = args.pop(0)
        log.debug('Added `%s` design document to cache', ddoc_id)
        return True
    else:
        ddoc = ddocs.get(ddoc_id)
        log.debug('Initiate work with `%s` design document', ddoc_id)
        if ddoc is None:
            log.error('Uncached design doc `%s`' % ddoc_id)
            raise FatalError('query_protocol_error',
                             'uncached design doc: %s' % ddoc_id)
        fun_path = args.pop(0)
        cmd = fun_path[0]
        func_args = args.pop(0)
        log.debug('Processing DDoc command `%s`', cmd)
        if cmd not in commands:
            log.error('Unknown ddoc command `%s`' % cmd)
            raise FatalError('unknown_command',
                             'unknown ddoc command `%s`' % cmd)
        handler = commands[cmd]
        point = ddoc
        for item in fun_path:
            prev, point = point, point.get(item)
            if point is None:
                msg = 'Missing %s function %s on design doc %s'
                log.error(msg, cmd, item, ddoc_id)
                raise Error('not_found', msg % (cmd, item, ddoc_id))
        else:
            func = point
            if type(func) is not FunctionType:
                func = compile_func(func, ddoc)
                prev[item] = func
        return handler(func, *func_args)
