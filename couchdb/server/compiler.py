# -*- coding: utf-8 -*-
#
"""Proceeds query server function compilation within special context."""
import base64
import os
import logging
import tempfile
from codecs import BOM_UTF8
from types import CodeType, FunctionType
from types import ModuleType
from couchdb.server.exceptions import Error, FatalError, Forbidden
from couchdb import json

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

__all__ = ['compile_func', 'require', 'DEFAULT_CONTEXT']

log = logging.getLogger(__name__)

DEFAULT_CONTEXT = {
    'Error': Error,
    'FatalError': FatalError,
    'Forbidden': Forbidden,
    'json': json,
}


class EggExports(dict):
    """Sentinel for egg export statements."""


def compile_to_bytecode(funsrc):
    """Compiles function source string to bytecode"""
    log.debug('Compile source code to function\n%s', funsrc)
    assert isinstance(funsrc, basestring), 'Invalid source object %r' % funsrc

    if isinstance(funsrc, unicode):
        funsrc = funsrc.encode('utf-8')
    if not funsrc.startswith(BOM_UTF8):
        funsrc = BOM_UTF8 + funsrc

    # compile + exec > exec
    return compile(funsrc.replace('\r\n', '\n'), '<string>', 'exec')

def maybe_b64egg(b64str):
    """Checks if passed string is base64 encoded egg file"""
    # Quick and dirty check for base64 encoded zipfile.
    # Saves time and IO operations in most cases.
    return isinstance(b64str, basestring) and b64str.startswith('UEsDBBQAAAAIA')

def maybe_export_egg(source, allow_eggs=False, egg_cache=None):
    """Tries to extract export statements from encoded egg"""
    if allow_eggs and maybe_b64egg(source):
        return import_b64egg(source, egg_cache)
    return None

def maybe_compile_function(source):
    """Tries to compile Python source code to bytecode"""
    if isinstance(source, basestring):
        return compile_to_bytecode(source)
    return None

def maybe_export_bytecode(source, context):
    """Tries to extract export statements from executed bytecode source"""
    if isinstance(source, CodeType):
        exec source in context
        return context.get('exports', {})
    return None

def maybe_export_cached_egg(source):
    """Tries to extract export statements from cached egg namespace"""
    if isinstance(source, EggExports):
        return source
    return None

def cache_to_ddoc(ddoc, path, obj):
    """Cache object to ddoc by specified path"""
    assert path, 'Path should not be empty'
    point = ddoc
    for item in path:
        prev, point = point, point.get(item)
    prev[item] = obj

def resolve_module(names, mod, root=None):
    def helper():
        return ('\n    id: %r'
                '\n    names: %r'
                '\n    parent: %r'
                '\n    current: %r'
                '\n    root: %r') % (idx, names, parent, current, root)
    idx = mod.get('id')
    parent = mod.get('parent')
    current = mod.get('current')
    if not names:
        if not isinstance(current, (basestring, CodeType, EggExports)):
            raise Error('invalid_require_path',
                        'Must require Python string, code object or egg cache,'
                        ' not %r (at %s)' % (type(current), idx))
        log.debug('Found object by id %s', idx)
        return {
            'current': current,
            'parent': parent,
            'id': idx,
            'exports': {}
        }
    log.debug('Resolving module at %s, remain path: %s', (idx, names))
    name = names.pop(0)
    if not name:
        raise Error('invalid_require_path',
                    'Required path shouldn\'t starts with slash character'
                    ' or contains sequence of slashes.' + helper())
    if name == '..':
        if parent is None or parent.get('parent') is None:
            raise Error('invalid_require_path',
                        'Object %r has no parent.' % idx + helper())
        return resolve_module(names, {
            'id': idx[:idx.rfind('/')],
            'parent': parent.get('parent'),
            'current': parent.get('current'),
        })
    elif name == '.':
        if parent is None:
            raise Error('invalid_require_path',
                        'Object %r has no parent.' % idx + helper())
        return resolve_module(names, {
            'id': idx,
            'parent': parent,
            'current': current,
        })
    elif root:
        idx = None
        mod = {'current': root}
        current = root
    if current is None:
        raise Error('invalid_require_path',
                    'Required module missing.' + helper())
    if not name in current:
        raise Error('invalid_require_path',
                    'Object %r has no property %r' % (idx, name) + helper())
    return resolve_module(names, {
        'current': current[name],
        'parent': mod,
        'id': (idx is not None) and (idx + '/' + name) or name
    })

def import_b64egg(b64str, egg_cache=None):
    """Imports top level namespace from base64 encoded egg file.

    For Python 2.4 `setuptools <http://pypi.python.org/pypi/setuptools>`_
    package required.

    :param b64str: Base64 encoded egg file.
    :type b64str: str

    :return: Egg top level namespace or None if egg import disabled.
    :rtype: dict
    """
    if iter_modules is None:
        raise ImportError('No tools available to work with eggs.'
                          ' Probably, setuptools package could solve'
                          ' this problem.')
    egg = None
    egg_path = None
    egg_cache = (egg_cache
                 or os.environ.get('PYTHON_EGG_CACHE')
                 or os.path.join(tempfile.gettempdir(), '.python-eggs'))
    try:
        try:
            if not os.path.exists(egg_cache):
                os.mkdir(egg_cache)
            hegg, egg_path = tempfile.mkstemp(dir=egg_cache)
            egg = os.fdopen(hegg, 'wb')
            egg.write(base64.b64decode(b64str))
            egg.close()
            exports = EggExports(
                [(name, loader.load_module(name))
                 for loader, name, ispkg in iter_modules([egg_path])]
            )
        except:
            log.exception('Egg import failed')
            raise
        else:
            if not exports:
                raise Error('egg_error', 'Nothing to export')
            return exports
    finally:
        if egg_path is not None and os.path.exists(egg_path):
            os.unlink(egg_path)

def require(ddoc, context=None, **options):
    """Wraps design ``require`` function with access to design document.

    :param ddoc: Design document.
    :type ddoc: dict

    :return: Require function object.

    Require function extracts export statements from stored module within
    design document. It could be used to access shared libraries of common used
    functions, however it's available only for DDoc function set.

    This function is from CommonJS world and works by detailed
    `specification <http://wiki.commonjs.org/wiki/Modules/1.1.1>`_.

    :param path: Path to stored module through document structure fields.
    :type path: basestring

    :param module: Current execution context. Normally, you wouldn't used this
        argument.
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
    .. versionchanged:: 1.1.0 Available for map functions.
    """
    context = context or DEFAULT_CONTEXT.copy()
    _visited_ids = []
    def require(path, module=None):
        log.debug('Looking for export objects at %s', path)
        module = module and module.get('parent') or {}
        new_module = resolve_module(path.split('/'), module, ddoc)

        if new_module['id'] in _visited_ids:
            log.error('Circular require calls have created deadlock!'
                      ' DDoc id `%s` ; call stack: %r',
                      ddoc.get('_id'), _visited_ids)
            del _visited_ids[:]
            raise RuntimeError('Require function calls have created deadlock')
        _visited_ids.append(new_module['id'])
        source = new_module['current']

        module_context = context.copy()
        module_context.update({
            'module': new_module,
            'exports': new_module['exports'],
        })
        module_context['require'] = lambda path: require(path, new_module)
        enable_eggs = options.get('enable_eggs', False)
        egg_cache = options.get('egg_cache', None)

        try:
            exports = maybe_export_egg(source, enable_eggs, egg_cache)
            if exports is not None:
                cache_to_ddoc(ddoc, new_module['id'].split('/'), exports)
                return exports

            exports = maybe_export_cached_egg(source)
            if exports is not None:
                return exports

            bytecode = maybe_compile_function(source)
            if bytecode is not None:
                cache_to_ddoc(ddoc, new_module['id'].split('/'), bytecode)
                source = bytecode
            try:
                exports = maybe_export_bytecode(source, module_context)
                if exports is not None:
                    return exports
            except Exception, err:
                log.exception('Failed to compile source code:\n%s',
                              new_module['current'])
                raise Error('compilation_error', str(err))
            
            raise Error('invalid_required_object', repr(new_module['current']))
        finally:
            if _visited_ids:
                _visited_ids.pop()
    return require

def compile_func(funsrc, ddoc=None, context=None, **options):
    """Compile source code and extract function object from it.

    :param funsrc: Python source code.
    :type funsrc: unicode

    :param ddoc: Optional argument which must represent design document.
    :type ddoc: dict

    :param context: Custom context objects which function could operate with.
    :type context: dict

    :param options: Compiler config options.

    :return: Function object.

    :raises:
        - :exc:`~couchdb.server.exceptions.Error`
          If source code compilation failed or it doesn't contains function
          definition.

    .. note::
        ``funsrc`` should contains only one function definition and import
        statements (optional) or :exc:`~couchdb.server.exceptions.Error`
        will be raised.

    """
    if not context:
        context = DEFAULT_CONTEXT.copy()
    else:
        context, _ = DEFAULT_CONTEXT.copy(), context
        context.update(_)
    if ddoc is not None:
        context['require'] = require(ddoc, context, **options)

    globals_ = {}
    try:
        bytecode = compile_to_bytecode(funsrc)
        exec bytecode in context, globals_
    except Exception, err:
        log.exception('Failed to compile source code:\n%s', funsrc)
        raise Error('compilation_error', str(err))

    msg = None
    func = None
    for item in globals_.values():
        if isinstance(item, FunctionType):
            if func is None:
                func = item
            else:
                msg = 'Multiple functions are defined. Only one is allowed.'
        elif not isinstance(item, ModuleType):
            msg = 'Only functions could be defined at top level namespace'
        if msg is not None:
            break
    if msg is None and not isinstance(func, FunctionType):
        msg = 'Expression does not eval to a function'
    if msg is not None:
        log.error('%s\n%s', msg, funsrc)
        raise Error('compilation_error', msg)
    return func
