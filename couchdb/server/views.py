# -*- coding: utf-8 -*-
#
import copy
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
    log.debug('Apply map functions to document `%s`:\n%s', docid, doc)
    orig_doc = copy.deepcopy(doc)
    map_results = []
    _append = map_results.append
    try:
        for idx, func in enumerate(server.state['functions']):
            # TODO: https://issues.apache.org/jira/browse/COUCHDB-729
            # Apply copy.deepcopy for `key` and `value` to fix this issue
            _append([[key, value] for key, value in func(doc) or []])
            if doc != orig_doc:
                log.warning('Document `%s` had been changed by map function'
                            ' `%s`, but was restored to original state',
                            docid, func.__name__)
                doc = copy.deepcopy(orig_doc)
    except Exception, err:
        msg = 'Exception raised for document `%s`:\n%s\n\n%s\n\n'
        funsrc = server.state['functions_src'][idx]
        log.exception(msg, docid, doc, funsrc)
        if isinstance(err, ViewServerException):
            raise
        # TODO: https://issues.apache.org/jira/browse/COUCHDB-282
        # Raise FatalError to fix this issue
        raise Error(err.__class__.__name__, str(err))
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
    _append = reductions.append
    keys, values = rereduce and (None, kvs) or zip(*kvs) or ([], [])
    log.debug('Reducing\nkeys: %s\nvalues: %s', keys, values)
    args = (keys, values, rereduce)
    try:
        for funsrc in reduce_funs:
            function = server.compile(funsrc)
            _append(function(*args[:function.func_code.co_argcount]))
    except Exception, err:
        msg = 'Exception raised on reduction:\nkeys: %s\nvalues: %s\n\n%s\n\n'
        log.exception(msg, keys, values, funsrc)
        if isinstance(err, ViewServerException):
            raise
        raise Error(err.__class__.__name__, str(err))

    # if-based pyramid was made by optimization reasons
    if server.is_reduce_limited():
        reduce_line = json.encode(reductions)
        reduce_len = len(reduce_line)
        if reduce_len > 200:
            size_overflowed = (reduce_len * 2) > len(json.encode(kvs))
            if size_overflowed:
                msg = ('Reduce output must shrink more rapidly:\n'
                       'Current output: `%s`... (first 100 of %d bytes)'
                       '') % (reduce_line[:100], reduce_len)
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
    log.debug('Rereducing values:\n%s', values)
    return reduce(server, reduce_funs, values, rereduce=True)
