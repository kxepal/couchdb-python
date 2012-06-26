# -*- coding: utf-8 -*-
#
import logging
from types import FunctionType
from couchdb.server.exceptions import FatalError, Error

__all__ = ['DDoc']

log = logging.getLogger(__name__)

class DDoc(object):
    """Design document operation class.

    :param commands: Mapping of commands to their callable handlers. Each
                     command actually is the first item in design function path.
                     See :meth:`run_ddoc_func` for more information.
    :type commands: dict

    :param others: Commands defined in keyword style. Have higher priority above
                   `commands` variable.
    """
    def __init__(self, commands=None, **others):
        if commands is None:
            commands = {}
        assert isinstance(commands, dict)
        commands.update(others)
        self.commands = commands
        self.cache = {}

    def __call__(self, *args, **kwargs):
        return self.process_request(*args, **kwargs)

    def process_request(self, server, cmd, *args):
        """Processes design functions stored within design documents."""
        if cmd == 'new':
            return self.add_ddoc(server, *args)
        else:
            return self.run_ddoc_func(server, cmd, *args)


    def add_ddoc(self, server, ddoc_id, ddoc):
        """
        :param server: Query server instance.
        :type server: :class:`~couchdb.server.BaseQueryServer`

        :param ddoc_id: Design document id.
        :type ddoc_id: unicode

        :param ddoc: Design document itself.
        :type ddoc: dict

        :return: True

        .. versionadded:: 0.11.0
        """
        log.debug('Cache design document `%s`', ddoc_id)
        self.cache[ddoc_id] = ddoc
        return True

    def run_ddoc_func(self, server, ddoc_id, fun_path, fun_args):
        """
        :param server: Query server instance.
        :type server: :class:`~couchdb.server.BaseQueryServer`

        :param ddoc_id: Design document id, holder of requested function.
        :type ddoc_id: unicode

        :param fun_path: List of key by which request function could be found
                         within ddoc object. First element of this list is
                         ddoc command.
        :type fun_path: list

        :param fun_args: List of function arguments.
        :type fun_args: list

        :return: Result of called design function if any available.
                 For example, lists doesn't explicitly returns any value.

        .. versionadded:: 0.11.0
        """
        ddoc = self.cache.get(ddoc_id)
        if ddoc is None:
            msg = 'Uncached design document: %s' % ddoc_id
            log.error(msg)
            raise FatalError('query_protocol_error', msg)
        cmd = fun_path[0]
        if cmd not in self.commands:
            msg = 'Unknown ddoc command `%s`' % cmd
            log.error(msg)
            raise FatalError('unknown_command', msg)
        handler = self.commands[cmd]
        point = ddoc
        for item in fun_path:
            prev, point = point, point.get(item)
            if point is None:
                msg = 'Missed function `%s` in design doc `%s` by path: %s'
                args = (item, ddoc_id, '/'.join(fun_path))
                log.error(msg, *args)
                raise Error('not_found', msg % args)
        else:
            func = point
            if not isinstance(func, FunctionType):
                func = server.compile(func, ddoc)
                prev[item] = func
        log.debug('Run %s in design doc `%s` by path: %s',
                  func, ddoc_id, '/'.join(fun_path))
        return handler(server, func, *fun_args)
