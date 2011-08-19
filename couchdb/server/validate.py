# -*- coding: utf-8 -*-
#
import logging
from couchdb.server.exceptions import Forbidden, Error, ViewServerException

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
    except ViewServerException, err:
        log.exception('%s exception raised by %s validate_doc_update function'
                      '' % (err.__class__.__name__, func.__name__))
        raise
    except Exception, err:
        log.exception('Something went wrong at %s validate_doc_update function'
                      '' % func.__name__)
        raise Error(err.__class__.__name__, str(err))
    return 1

def validate(server, funsrc, newdoc, olddoc, userctx):
    """Implementation of `validate` command.

    :command: validate

    :param funsrc: validate_doc_update function source.
    :type funsrc: unicode

    :param newdoc: New document version.
    :type newdoc: dict

    :param olddoc: Stored document version.
    :type olddoc: dict

    :param userctx: User info.
    :type userctx: dict

    :return: 1 (number one)
    :rtype: int

    .. versionadded:: 0.9.0
    .. deprecated:: 0.11.0
        Now is a subcommand of :ref:`ddoc`.
        Use :func:`~couchdb.server.validate.ddoc_validate` instead.
    """
    return run_validate(server.compile(funsrc), newdoc, olddoc, userctx)

def ddoc_validate(server, func, newdoc, olddoc, userctx, secobj=None):
    """Implementation of ddoc `validate_doc_update` command.

    :command: validate_doc_update

    :param func: validate_doc_update function.
    :type func: function

    :param newdoc: New document version.
    :type newdoc: dict

    :param olddoc: Stored document version.
    :type olddoc: dict

    :param userctx: User info.
    :type userctx: dict

    :param secobj: Database security information.
    :type secobj: dict

    :return: 1 (number one)
    :rtype: int

    .. versionadded:: 0.9.0
    .. versionchanged:: 0.11.1 Added argument ``secobj``.
    """
    args = newdoc, olddoc, userctx, secobj
    if server.version >= (0, 11, 1):
        if func.func_code.co_argcount == 3:
            log.warning('Since 0.11.1 CouchDB validate_doc_update functions'
                        ' takes additional 4th argument `secobj`.'
                        ' Please, update your code to remove this warning.')
            args = args[:3]
    else:
        args = args[:3]
    return run_validate(func, *args)
