# -*- coding: utf-8 -*-
#
'''Controls all workflow with input/output streams'''
import logging
import sys
from couchdb import json
from couchdb.server.exceptions import FatalError

__all__ = ['input', 'output', 'receive', 'respond']

log = logging.getLogger(__name__)

#: Input data stream. By default: sys.stdin
input = sys.stdin
#: Output data stream. By default: sys.stdout
output = sys.stdout
last_line = ''

def receive():
    '''Yields json decoded line from input stream.

    :yields: JSON decoded object.
    :rtype: list
    '''
    global last_line
    while True:
        line = input.readline()
        last_line = line
        if not line:
            break
        yield json.decode(line)

def respond(obj):
    '''Writes json encoded object to output stream.

    :param obj: JSON encodable object.
    :type obj: dict or list
    '''
    try:
        obj = json.encode(obj)
    except ValueError, err:
        log.exception('Error converting %r to json', obj)
        raise FatalError('json_encode', str(err))
    else:
        if isinstance(obj, unicode):
            obj = obj.encode('utf-8')
        output.write(obj)
        output.write('\n')
        try:
            output.flush()
        except IOError:
            # This could happened if query server process have been terminated
            # unexpectable. Probably, this exception is not one that would
            # care us in such situation.
            pass