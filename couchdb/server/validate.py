# -*- coding: utf-8 -*-
#
import logging
from couchdb.server import state
from couchdb.server.compiler import compile_func
from couchdb.server.exceptions import Forbidden

__all__ = ['validate', 'ddoc_validate']

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

def validate(func, newdoc, olddoc, userctx):
    '''Implemention of `validate` command.

    :command: validate

    :param func: validate_doc_update function source.
    :param newdoc: New document version as dict.
    :param olddoc: Stored document version as dict.
    :param userctx: User info dict.
    :type func: unicode
    :type newdoc: dict
    :type olddoc: dict
    :type userctx: dict

    :return: 1 (number one)
    :rtype: int

    .. versionadded:: 0.9.0
    .. deprecated:: 0.11.0
        Now is a subcommand of :ref:`ddoc`.
        Use :func:`~couchdb.server.validate.ddoc_validate` instead.
    '''
    return run_validate(compile_func(func), newdoc, olddoc, userctx)

def ddoc_validate(func, newdoc, olddoc, userctx, secobj=None):
    '''Implemention of ddoc `validate_doc_update` command.

    :command: validate_doc_update

    :param func: validate_doc_update function.
    :param newdoc: New document version as dict.
    :param olddoc: Stored document version as dict.
    :param userctx: User info dict.
    :param secobj: Database security information dict.
    :type func: function
    :type newdoc: dict
    :type olddoc: dict
    :type userctx: dict
    :type secobj: dict

    :return: 1 (number one)
    :rtype: int

    .. versionadded:: 0.9.0
    .. versionchanged:: 0.11.1 Added argument ``secobj``.
    '''
    args = newdoc, olddoc, userctx, secobj
    if state.version >= (0, 11, 1):
        if func.func_code.co_argcount == 3:
            log.warning('Since 0.11.1 CouchDB validate_doc_update functions'
                        ' takes additional 4th argument `secobj`.'
                        ' Please, update your code to remove this warning.')
            args = args[:3]
    else:
        args = args[:3]
    return run_validate(func, *args)