# -*- coding: utf-8 -*-
#
'''Holds Query Server state.'''
import logging
from couchdb.server.compiler import compile_func

__all__ = ['add_fun', 'add_lib', 'reset', 'line_length', 'functions',
           'functions_src', 'query_config', 'version', 'enable_eggs',
           'egg_cache', 'allow_get_update']

log = logging.getLogger(__name__)

view_lib = None
#: Line length of current input string
line_length = 0
#: List of functions objects placed by add_fun command.
functions = []
#: List of functions source code placed by add_fun command.
functions_src = []
#: Query server configuration.
query_config = {}
#: Version of CouchDB server with which query server is compatibile.
#: Default: latest implemented.
version = None
#: Controls eggs support feature
enable_eggs = False
#: Specify eggs cache path. If omitted, system tempdir would be used.
egg_cache = None
#: Allows 
allow_get_update = False

def reset(config=None):
    '''Resets view server state.

    :command: reset

    :param config: Optional dict argument to set up query config.
    :type config: dict

    :return: True
    :rtype: bool
    '''
    del functions[:]
    del functions_src[:]
    query_config.clear()
    if config is not None:
        query_config.update(config)
    return True

def add_fun(funstr):
    '''Compiles and adds function to state cache.

    :command: add_fun

    :param funstr: Python function as source string.
    :type funstr: basestring

    :return: True
    :rtype: bool
    '''
    if version >= (1, 1, 0):
        ddoc = {'views': {'lib': view_lib}}
        functions.append(compile_func(funstr, ddoc))
    else:
        functions.append(compile_func(funstr))
    functions_src.append(funstr)
    return True

def add_lib(lib):
    '''Add lib to state which could be used within views that allows usage
    require function within maps one to import shared objects.

    :command: add_lib

    :param lib: Python source code which used require function protocol.
    :type lib: basestring

    :return: True
    :rtype: bool

    .. versionadded:: 1.1.0
    '''
    global view_lib
    view_lib = lib
    return True
