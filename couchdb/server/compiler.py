# -*- coding: utf-8 -*-
#
'''Proceeds query server function compilation within special context.'''
import logging
from codecs import BOM_UTF8
from types import FunctionType
from types import ModuleType
from couchdb.server.exceptions import Error

__all__ = ['compile_func', 'require', 'context']

log = logging.getLogger(__name__)

context = {}

_code_type = type(compile('', '<string>', 'exec'))

def resolve_module(names, mod, root=None):
    def helper():
        return ('\n    id: %r'
                '\n    names: %r'
                '\n    parent: %r'
                '\n    current: %r'
                '\n    root: %r') % (id, names, parent, current, root)
    id = mod.get('id')
    parent = mod.get('parent')
    current = mod.get('current')
    if not names:
        if not isinstance(current, (basestring, _code_type)):
            raise Error('invalid_require_path',
                        'Must require Python string, not\n%r' % current)
        return {
            'current': current,
            'parent': parent,
            'id': id,
            'exports': {}
        }
    n = names.pop(0)
    if n == '..':
        if parent is None or parent.get('parent') is None:
            raise Error('invalid_require_path',
                        'Object %r has no parent.' % id + helper())
        return resolve_module(names, {
            'id': id[:id.rfind('/')],
            'parent': parent['parent'].get('parent'),
            'current': parent['parent'].get('current')
        })
    elif n == '.':
        if parent is None:
            raise Error('invalid_require_path',
                        'Object %r has no parent.' % id + helper())
        return resolve_module(names, {
            'id': id,
            'parent': parent.get('parent'),
            'current': parent.get('current'),
        })
    elif not n:
        raise Error('invalid_require_path',
                    'Required path shouldn\'t starts with slash character'
                    ' or contains sequence of slashes.' + helper())
    elif root:
        mod, current = {'current': root}, root
    if current is None:
        raise Error('invalid_require_path',
                    'Required module missing.' + helper())
    if not n in current:
        raise Error('invalid_require_path',
                    'Object %r has no property %r' % (id, n) + helper())
    return resolve_module(names, {
        'current': current[n],
        'parent': mod,
        'id': (id is not None and id != n) and (id + '/' + n) or n
    })

def require(ddoc):
    '''Wraps design ``require`` function with access to design document.

    :param ddoc: Design document.
    :type ddoc: dict

    :return: Require function object.

    Require function extracts export statements from stored module within
    design document. It could be used to access shared libriaries of common used
    functions, however it's avaliable only for DDoc function set.

    :param path: Path to stored module throught document structure fields.
    :param module: Current execution context. Normaly, you wouldn't used this
        argument.
    :type path: basestring
    :type module: dict

    :return: Exported statements.
    :rtype: dict

    Example of stored module:
        >>> class Validate(object):
        >>>     def __init__(self, newdoc, olddoc, userctx):
        >>>         self.newdoc = newdoc
        >>>         self.olddoc = olddoc
        >>>         self.userctx = userctx
        >>>
        >>>     def is_author():
        >>>         return self.doc['author'] == self.userctx['name']
        >>>
        >>>     def is_admin():
        >>>         return '_admin' in self.userctx['roles']
        >>>
        >>>     def unchanged(field):
        >>>         assert (self.olddoc is not None
        >>>                 and self.olddoc[field] == self.newdoc[field])
        >>>
        >>> exports['init'] = Validate

    Example of usage:
        >>> def validate_doc_update(newdoc, olddoc, userctx):
        >>>     init_v = require('lib/validate')['init']
        >>>     v = init_v(newdoc, olddoc, userctx)
        >>>
        >>>     if v.is_admin():
        >>>         return True
        >>>
        >>>     v.unchanged('author')
        >>>     v.unchanged('created_at')
        >>>     return True

    .. versionadded:: 0.11.0
    .. versionchanged:: 1.1.0 Avaiable for map functions if add_lib
        command proceeded.
    '''
    _visited_ids = []
    def require(path, module=None):
        log.debug('Importing objects from %s', path)
        module = module or {}
        new_module = resolve_module(path.split('/'), module, ddoc)
        if new_module['id'] in _visited_ids:
            raise RuntimeError('Circular require calls deadlock occured')
        _visited_ids.append(new_module['id'])
        source = new_module['current']
        globals_ = {
            'module': new_module,
            'exports': new_module['exports'],
        }
        module_context = context.copy()
        module_context['require'] = lambda path: require(path, new_module)
        try:
            if isinstance(source, basestring):
                bytecode = compile(source, '<string>', 'exec')
                point = ddoc
                for item in new_module['id'].split('/'):
                    prev, point = point, point.get(item)
                prev[item] = bytecode
            else:
                bytecode = source
            exec bytecode in module_context, globals_
        except Exception, err:
            raise Error('compilation_error', '%s:\n%s' % (err, source))
        else:
            _visited_ids.pop()
            return globals_['exports']
    return require

def compile_func(funstr, ddoc=None):
    '''Compile source code and extract function object from it.

    :param funstr: Python source code.
    :param ddoc: Optional argument which must represent design document.
    :type funstr: unicode
    :type ddoc: dict

    :return: Function object.

    :raise Error: If compilation was not succeeded.

    .. note:: ``funstr`` should contains only one function definition and nothing
        else except imported modules. Otherwise Error exception would be raised.
    '''
    log.debug('Compiling code to function:\n%s', funstr)
    funstr = BOM_UTF8 + funstr.encode('utf-8')
    globals_ = {}
    if ddoc is not None:
        context['require'] = require(ddoc)
    elif 'require' in context:
        context.pop('require')
    try:
        # compile + exec > exec
        bytecode = compile(funstr, '<string>', 'exec')
        exec bytecode in context, globals_
    except Exception, err:
        raise Error('compilation_error', '%s:\n%s' % (err, funstr))
    msg = None
    func = None
    for item in globals_.values():
        if isinstance(item, FunctionType):
            if func is None:
                func = item
            else:
                msg = 'Mutiple functions are defined. Only one is allowed.'
        elif not isinstance(item, ModuleType):
            msg = 'Only functions could be defined at top level namespace'
        if msg is not None:
            break
    if msg is None and not isinstance(func, FunctionType):
        msg = 'Expression does not eval to a function'
    if msg is not None:
        raise Error('compilation_error', '%s\n%s' % (msg, funstr))
    return func
