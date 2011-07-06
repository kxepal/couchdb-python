# -*- coding: utf-8 -*-
#
'''Proceeds query server function compilation within special context.'''
import base64
import os
import logging
import tempfile
from codecs import BOM_UTF8
from types import FunctionType
from types import ModuleType
from couchdb.server.exceptions import Error
try:
    from pkgutil import iter_modules
except ImportError:
    try:
        # Python 2.4
        from pkg_resources import get_importer, zipimport
        def iter_modules(paths):
            for path in paths:
                loader = get_importer(path)
                if not isinstance(loader, zipimport.zipimporter):
                    continue
                names = loader.get_data('EGG-INFO/top_level.txt')
                for name in names.split('\n')[:-1]:
                    yield loader, name, None
    except ImportError:
        get_importer = None
        iter_modules = None
        zipimport = None

__all__ = ['compile_func', 'require', 'context']

log = logging.getLogger(__name__)
state = None
context = {}

_code_type = type(compile('', '<string>', 'exec'))

def resolve_module(names, mod, root=None):
    def helper():
        return ('\n    id: %r'
                '\n    names: %r'
                '\n    parent: %r'
                '\n    current: %r'
                '\n    root: %r') % (id, names, parent, current, root)
    log.debug('Resolve module at %s. Current frame: %s', names, mod)
    id = mod.get('id')
    parent = mod.get('parent')
    current = mod.get('current')
    if not names:
        if not isinstance(current, (basestring, _code_type)):
            raise Error('invalid_require_path',
                        'Must require Python string or code object,'
                        ' not %r' % current)
        return {
            'current': current,
            'parent': parent,
            'id': id,
            'exports': {}
        }
    n = names.pop(0)
    if not n:
        raise Error('invalid_require_path',
                    'Required path shouldn\'t starts with slash character'
                    ' or contains sequence of slashes.' + helper())
    if n == '..':
        if parent is None or parent.get('parent') is None:
            raise Error('invalid_require_path',
                        'Object %r has no parent.' % id + helper())
        return resolve_module(names, {
            'id': id[:id.rfind('/')],
            'parent': parent.get('parent'),
            'current': parent.get('current'),
        })
    elif n == '.':
        if parent is None:
            raise Error('invalid_require_path',
                        'Object %r has no parent.' % id + helper())
        return resolve_module(names, {
            'id': id,
            'parent': parent,
            'current': current,
        })
    elif root:
        id = None
        mod = {'current': root}
        current = root
    if current is None:
        raise Error('invalid_require_path',
                    'Required module missing.' + helper())
    if not n in current:
        raise Error('invalid_require_path',
                    'Object %r has no property %r' % (id, n) + helper())
    return resolve_module(names, {
        'current': current[n],
        'parent': mod,
        'id': (id is not None) and (id + '/' + n) or n
    })

def import_b64egg(b64egg):
    '''Imports top level namespace from base64 encoded egg file.

    For Python 2.4 `setuptools <http://pypi.python.org/pypi/setuptools>`_
    package required.

    :param b64egg: Base64 encoded egg file.
    :type b64egg: str

    :return: Egg top level namespace or None if egg import disabled.
    :rtype: dict
    '''
    global state
    # Quick and dirty check for base64 encoded zipfile.
    # Saves time and IO operations in most cases.
    if not b64egg.startswith('UEsDBBQAAAAIA'):
        return None
    if state is None:
        # circular import dependency resolve
        from couchdb.server import state
    if not state.enable_eggs:
        return None
    egg = None
    exports = None
    egg_cache = (state.egg_cache
                 or os.environ.get('PYTHON_EGG_CACHE')
                 or os.path.join(tempfile.gettempdir(), '.python-eggs'))
    try:
        try:
            if iter_modules is None:
                raise ImportError('No tools available to work with eggs.'
                                  ' Probably, setuptools package could solve'
                                  ' this problem.')
            if not os.path.exists(egg_cache):
                os.mkdir(egg_cache)
            hegg, egg_path = tempfile.mkstemp(dir=egg_cache)
            egg = os.fdopen(hegg, 'wb')
            egg.write(base64.b64decode(b64egg))
            egg.close()
            exports = dict(
                [(name, loader.load_module(name))
                 for loader, name, ispkg in iter_modules([egg_path])]
            )
        except:
            log.exception('Egg import failed')
            raise
        else:
            return exports
    finally:
        if egg is not None and os.path.exists(egg_path):
            os.unlink(egg_path)

def require(ddoc):
    '''Wraps design ``require`` function with access to design document.

    :param ddoc: Design document.
    :type ddoc: dict

    :return: Require function object.

    Require function extracts export statements from stored module within
    design document. It could be used to access shared libraries of common used
    functions, however it's available only for DDoc function set.

    This function is from CommonJS world and works by detailed
    `specification <http://wiki.commonjs.org/wiki/Modules/1.1.1>`_.

    :param path: Path to stored module through document structure fields.
    :param module: Current execution context. Normally, you wouldn't used this
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
    .. versionchanged:: 1.1.0 Available for map functions if add_lib
        command proceeded.
    '''
    _visited_ids = []
    def require(path, module=None):
        log.debug('Importing objects from %s', path)
        module = module and module.get('parent') or {}
        new_module = resolve_module(path.split('/'), module, ddoc)
        if new_module['id'] in _visited_ids:
            log.error('Circular require calls have created deadlock!'
                      ' DDoc id `%s` ; call stack: %r',
                      ddoc.get('_id'), _visited_ids)
            del _visited_ids[:]
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
            try:
                bytecode = None
                if isinstance(source, basestring):
                    egg = import_b64egg(source)
                    if egg is None:
                        bytecode = compile(source, '<string>', 'exec')
                    else:
                        exports = egg
                    point = ddoc
                    for item in new_module['id'].split('/'):
                        prev, point = point, point.get(item)
                    prev[item] = bytecode or egg
                elif isinstance(source, _code_type):
                    bytecode = source
                elif isinstance(source, dict):
                    exports = source
                if bytecode is not None:
                    exec bytecode in module_context, globals_
                    exports = globals_.get('exports', {})
            except Error, err:
                err.__init__('compilation_error', err.args[1])
                raise
            except Exception, err:
                log.exception('Compilation error')
                raise Error('compilation_error', '%s:\n%s' % (err, source))
            else:
                return exports
        finally:
            if _visited_ids:
                _visited_ids.pop()
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
        log.exception('Failed to compile function\n%s', funstr)
        raise Error('compilation_error', err)
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
        log.error('%s\n%s', msg, funstr)
        raise Error('compilation_error', '%s' % msg)
    return func
