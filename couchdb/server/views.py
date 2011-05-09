# -*- coding: utf-8 -*-
#
import logging
from couchdb import json
from couchdb.server import state
from couchdb.server.exceptions import ViewServerException, Error
from couchdb.server.compiler import compile_func

__all__ = ['map_doc', 'reduce', 'rereduce']

log = logging.getLogger(__name__)

def map_doc(doc):
    '''Applies available map functions to document.

    :command: map_doc

    :param doc: Document object.
    :type doc: dict

    :return: List of key-value results for each applied map function.

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If any Python exception occurres due mapping.
    '''
    docid = doc.get('_id')
    log.debug('Running map functions for doc._id `%s`', docid)
    map_results = []
    orig_doc = doc.copy()
    for i, function in enumerate(state.functions):
        try:
            result = [[key, value] for key, value in function(doc) or []]
        except ViewServerException:
            log.exception('Query server exception occured, aborting operation')
            raise
        except Exception, err:
            msg = 'Map function raised error for doc._id `%s`\n%s\n'
            funstr = state.functions_src[i]
            log.exception(msg, docid, funstr)
            raise Error(err.__class__.__name__, msg % (docid, err))
        else:
            map_results.append(result)
        # quick and dirty trick to prevent document from changing
        # within map functions.
        if doc != orig_doc:
            log.warning("Document `%s` had been changed by map function "
                        "'%s', but was restored to original state",
                        doc.get('_id'), function.__name__)
            doc = orig_doc.copy()
    return map_results

def reduce(reduce_funs, kvs, rereduce=False):
    '''Reduces mapping result.

    :command: reduce

    :param reduce_funs: List of reduce functions source code.
    :param kvs: List of key-value pairs.
    :param rereduce: Sign of rereduce mode.
    :type reduce_funs: list
    :type kvs: list
    :type rereduce: bool

    :return: Two element list with True and reduction result.
    :rtype: list

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If any Python exception occurres or reduce ouput is twice longer
          as state.line_length and reduce_limit is enabled in state.query_config.
    '''
    reductions = []
    keys, values = rereduce and (None, kvs) or zip(*kvs) or ([],[])
    args = (keys, values, rereduce)
    for funstr in reduce_funs:
        try:
            function = compile_func(funstr)
            result = function(*args[:function.func_code.co_argcount])
        except ViewServerException:
            log.exception('Query server exception occured, aborting operation')
            raise
        except Exception, err:
            msg = 'Reduce function raised an error'
            log.exception(msg)
            raise Error(err.__class__.__name__, msg + ':\n%s' % err)
        else:
            reductions.append(result)

    reduce_len = len(reductions)
    reduce_limit = state.query_config.get('reduce_limit', False)
    size_overflowed = (reduce_len * 2) > state.line_length

    if reduce_limit and reduce_len > 200  and size_overflowed:
        reduce_line = json.encode(reductions)
        msg = "Reduce output must shirnk more rapidly:\n"\
              "Current output: '%s'... "\
              "(first 100 of %d bytes)" % (reduce_line[:100], reduce_len)
        log.error(msg)
        raise Error('reduce_overflow_error', msg)
    return [True, reductions]

def rereduce(reduce_funs, values):
    '''Rereduces mapping result

    :command: rereduce

    :param reduce_funs: List of reduce functions source code.
    :param values: List values.
    :type reduce_funs: list
    :type values: list

    :return: Two element list with True and rereduction result.
    :rtype: list

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If any Python exception occurres or reduce ouput is twice longer
          as state.line_length and reduce_limit is enabled in state.query_config.
    '''
    return reduce(reduce_funs, values, rereduce=True)
