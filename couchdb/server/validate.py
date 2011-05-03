# -*- coding: utf-8 -*-
#
import logging
from couchdb.server import state
from couchdb.server.compiler import compile_func
from couchdb.server.exceptions import Forbidden

log = logging.getLogger(__name__)

def handle_error(err, userctx):
    if isinstance(err, Forbidden):
        reason = err.args[0]
        log.warn('Access deny for user %s. Reason: %s', userctx, reason)
        raise
    elif isinstance(err, AssertionError):
        # This is custom behavior that allows to use assert statement
        # for field validation. It's just quite handy.
        log.warn('Access deny for user %s. Reason: %s', userctx, err)
        raise Forbidden(str(err))

def run_validate(func, *args):
    try:
        func(*args)
    except (AssertionError, Forbidden), err:
        handle_error(err, args[2])
    return 1

def validate(func, *args):
    '''Implemention of `validate` / ddoc `validate_doc_update` commands.

    :command: validate / validate_doc_update

    :param func: validate_doc_update function.
    :param newdoc: New document version as dict.
    :param olddoc: Stored document version as dict.
    :param userctx: User info dict.
    :param secobj: Database security information dict.
    :type func: Function object
    :type newdoc: dict
    :type olddoc: dict
    :type userctx: dict
    :type secobj: dict

    :return: 1 (number one)
    :rtype: int

    .. versionadded:: 0.9.0
    .. versionchanged:: 0.11.0 ``func`` argument now passed as function object,
        not source basestring.
    .. versionchanged:: 0.11.0 Now is a subcommand of :ref:`ddoc` as
        `validate_doc_update`.
    .. versionchanged:: 0.11.1 Added argument ``secobj``.

    '''
    if state.version < (0, 11, 0):
        func = compile_func(func)
    if state.version >= (0, 11, 1):
        if func.func_code.co_argcount == 3:
            log.warning('Since 0.11.1 CouchDB validate_doc_update functions'
                        ' takes additional 4th argument `secobj`.'
                        ' Please, update your code to remove this warning.')
            args = args[:3]
    return run_validate(func, *args)
