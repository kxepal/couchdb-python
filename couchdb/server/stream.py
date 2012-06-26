# -*- coding: utf-8 -*-
#
"""Controls all workflow with input/output streams"""
import logging
import sys
from couchdb import json
from couchdb.server.exceptions import FatalError

__all__ = ['receive', 'respond']

log = logging.getLogger(__name__)

def receive(input=sys.stdin):
    """Yields json decoded line from input stream.

    :param input: Input stream with `.readline()` support.

    :yields: JSON decoded object.
    :rtype: list
    """
    while True:
        line = input.readline()
        if not line:
            break
        log.debug('Input:\n%r', line)
        try:
            yield json.decode(line)
        except Exception, err:
            log.exception('Unable to decode json data:\n%s', line)
            raise FatalError('json_decode', str(err))

def respond(obj, output=sys.stdout):
    """Writes json encoded object to output stream.

    :param obj: JSON encodable object.
    :type obj: dict or list

    :param output: Output file-like object.
    """
    if obj is None:
        log.debug('Nothing to respond')
        return
    try:
        obj = json.encode(obj) + '\n'
    except Exception, err:
        log.exception('Unable to encode object to json:\n%r', obj)
        raise FatalError('json_encode', str(err))
    else:
        if isinstance(obj, unicode):
            obj = obj.encode('utf-8')
        log.debug('Output:\n%r', obj)
        output.write(obj)
        output.flush()
