# -*- coding: utf-8 -*-
#
import logging

__all__ = ['add_fun', 'add_lib', 'reset']

log = logging.getLogger(__name__)

def reset(server, config=None):
    """Resets query server state.

    :command: reset

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param config: Optional dict argument to set up query config.
    :type config: dict

    :return: True
    :rtype: bool
    """
    del server.state['functions'][:]
    del server.state['functions_src'][:]
    server.state['query_config'].clear()
    if config is not None:
        server.state['query_config'].update(config)
    return True

def add_fun(server, funsrc):
    """Compiles and adds function to state cache.

    :command: add_fun

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param funsrc: Python function as source string.
    :type funsrc: basestring

    :return: True
    :rtype: bool
    """
    if server.version >= (1, 1, 0):
        ddoc = {'views': {'lib': server.state.get('view_lib', '')}}
    else:
        ddoc = None
    server.state['functions'].append(server.compile(funsrc, ddoc))
    server.state['functions_src'].append(funsrc)
    return True

def add_lib(server, lib):
    """Add lib to state which could be used within views that allows usage
    require function within maps one to import shared objects.

    :command: add_lib

    :param server: Query server instance.
    :type server: :class:`~couchdb.server.BaseQueryServer`

    :param lib: Python source code which used require function protocol.
    :type lib: basestring

    :return: True
    :rtype: bool

    .. versionadded:: 1.1.0
    """
    server.state['view_lib'] = lib
    return True
