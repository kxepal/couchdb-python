# -*- coding: utf-8 -*-
#
import logging
from couchdb import json
from couchdb.server.exceptions import ViewServerException, Error

__all__ = ['map_doc', 'reduce', 'rereduce']

log = logging.getLogger(__name__)

def map_doc(server, doc):
    """Applies available map functions to document.

    :command: map_doc

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param doc: Document object.
    :type doc: dict

    :return: List of key-value results for each applied map function.

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If any Python exception occurs due mapping.
    """
    docid = doc.get('_id')
    log.debug('Running map functions for doc._id `%s`', docid)
    map_results = []
    orig_doc = doc.copy()
    try:
        for idx, func in enumerate(server.state['functions']):
            result = [[key, value] for key, value in func(doc) or []]
            map_results.append(result)
            # quick and dirty trick to prevent document from changing
            # within map functions.
            if doc != orig_doc:
                log.warning("Document `%s` had been changed by map function "
                            "'%s', but was restored to original state",
                            doc.get('_id'), func.__name__)
                doc = orig_doc.copy()
    except ViewServerException:
        log.exception('Query server exception occurred, aborting operation')
        raise
    except Exception, err:
        msg = 'Map function raised error for doc._id `%s`\n%s\n'
        funsrc = server.state['functions_src'][idx]
        log.exception(msg, docid, funsrc)
        raise Error(err.__class__.__name__, msg % (docid, err))
    else:
        return map_results

def reduce(server, reduce_funs, kvs, rereduce=False):
    """Reduces mapping result.

    :command: reduce

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param reduce_funs: List of reduce function source codes.
    :type reduce_funs: list

    :param kvs: List of key-value pairs.
    :type kvs: list

    :param rereduce: Sign of rereduce mode.
    :type rereduce: bool

    :return: Two element list with True and reduction result.
    :rtype: list

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If any Python exception occurs or reduce output is twice longer
          as state.line_length and reduce_limit is enabled in state.query_config
    """
    reductions = []
    # If rereduce processed kvs variable contains only list of values, so we
    # have set keys to None. Otherwise kvs should be splitted to keys and values
    # lists by zip function, but it could return empty list if no documents was
    # emitted by map function. To prevent exception, empty lists should be
    # assigned explicitly to keys and values variables.
    keys, values = rereduce and (None, kvs) or zip(*kvs) or ([],[])
    args = (keys, values, rereduce)
    try:
        for funsrc in reduce_funs:
            function = server.compile(funsrc)
            result = function(*args[:function.func_code.co_argcount])
            reductions.append(result)
    except ViewServerException:
        log.exception('Query server exception occurred, aborting operation')
        raise
    except Exception, err:
        msg = 'Reduce function raised an error: %s\n' % funsrc
        log.exception(msg)
        raise Error(err.__class__.__name__, '%s:\n%s' % (msg, err))

    # if-based pyramid was made by optimization reasons
    if server.is_reduce_limited():
        reduce_line = json.encode(reductions)
        reduce_len = len(reduce_line)
        if reduce_len > 200:
            size_overflowed = (reduce_len * 2) > len(json.encode(kvs))
            if size_overflowed:
                msg = ("Reduce output must shrink more rapidly:\n"
                      "Current output: '%s'... (first 100 of %d bytes)"
                      "") % (reduce_line[:100], reduce_len)
                log.error(msg)
                raise Error('reduce_overflow_error', msg)
    return [True, reductions]

def rereduce(server, reduce_funs, values):
    """Rereduces mapping result

    :command: rereduce

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param reduce_funs: List of reduce functions source code.
    :type reduce_funs: list

    :param values: List values.
    :type values: list

    :return: Two element list with True and rereduction result.
    :rtype: list

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If any Python exception occurs or reduce output is twice longer
          as state.line_length and reduce_limit is enabled in state.query_config
    """
    return reduce(server, reduce_funs, values, rereduce=True)
