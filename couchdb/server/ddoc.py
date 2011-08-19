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
                     See :meth:`process_request` for more information.
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

    def process_request(self, server, *args, **kwargs):
        """Processes design functions stored within design documents.

        Also holds cache of design documents, but ddoc must have to be
        registered before proceeding.

        :command: ddoc

        To put ddoc into cache:

        :param new: String constant "new".
        :type new: unicode

        :param ddoc_id: Design document id.
        :type ddoc_id: unicode

        :param ddoc: Design document itself.
        :type ddoc: dict

        :return: True

        To call function from ddoc:

        :param ddoc_id: Design document id, holder of requested function.
        :type ddoc_id: unicode

        :param func_path: List of key by which request function could be found
                          within ddoc object. First element of this list is
                          ddoc command.
        :type func_path: list

        :param func_args: List of function arguments.
        :type func_args: list

        :return: If ddoc putted into cache True will be returned.
            If ddoc function called returns it's result if any exists.
            For example, lists doesn't explicitly returns any value.

        .. versionadded:: 0.11.0
        """
        args = list(args)
        ddoc_id = args.pop(0)
        if ddoc_id == 'new':
            ddoc_id = args.pop(0)
            self.cache[ddoc_id] = args.pop(0)
            log.debug('Added `%s` design document to cache', ddoc_id)
            return True
        else:
            ddoc = self.cache.get(ddoc_id)
            log.debug('Initiate work with `%s` design document', ddoc_id)
            if ddoc is None:
                log.error('Uncached design doc `%s`' % ddoc_id)
                raise FatalError('query_protocol_error',
                                 'uncached design doc: %s' % ddoc_id)
            fun_path = args.pop(0)
            cmd = fun_path[0]
            func_args = args.pop(0)
            log.debug('Processing DDoc command `%s`', cmd)
            if cmd not in self.commands:
                log.error('Unknown ddoc command `%s`' % cmd)
                raise FatalError('unknown_command',
                                 'unknown ddoc command `%s`' % cmd)
            handler = self.commands[cmd]
            point = ddoc
            for item in fun_path:
                prev, point = point, point.get(item)
                if point is None:
                    msg = 'Missing %s function %s on design doc %s'
                    log.error(msg, cmd, item, ddoc_id)
                    raise Error('not_found', msg % (cmd, item, ddoc_id))
            else:
                func = point
                if type(func) is not FunctionType:
                    func = server.compile(func, ddoc)
                    prev[item] = func
            return handler(server, func, *func_args)
