# -*- coding: utf-8 -*-
#
"""Controls all workflow with input/output streams"""
import logging
import sys
from couchdb import json
from couchdb.server import state
from couchdb.server.exceptions import FatalError

__all__ = ['input', 'output', 'receive', 'respond', 'last_line']

log = logging.getLogger(__name__)

#: Input data stream. By default: sys.stdin
input = sys.stdin
#: Output data stream. By default: sys.stdout
output = sys.stdout

def receive():
    """Yields json decoded line from input stream.

    :yields: JSON decoded object.
    :rtype: list
    """
    while True:
        line = input.readline()
        state.line_length = len(line)
        if not line:
            log.debug('No more data in stream.')
            break
        log.debug('Data received:\n%s', line)
        try:
            yield json.decode(line)
        except Exception, err:
            log.exception('Unable to decode json data: %s', line)
            raise FatalError('json_decode', str(err))

def respond(obj):
    """Writes json encoded object to output stream.

    :param obj: JSON encodable object.
    :type obj: dict or list
    """
    log.debug('Data to respond:\n%s', obj)
    try:
        obj = json.encode(obj)
    except Exception, err:
        log.exception('Error converting %r to json', obj)
        raise FatalError('json_encode', str(err))
    else:
        if isinstance(obj, unicode):
            obj = obj.encode('utf-8')
        log.debug('Responding:\n%s', obj)
        output.write(obj)
        output.write('\n')
        try:
            output.flush()
        except IOError:
            log.exception('IOError occurred while output flushing.')
            # This could happened if query server process have been terminated
            # unexpectable. Probably, this exception is not one that would
            # care us in such situation.
            pass
