# -*- coding: utf-8 -*-
#
from inspect import getsource
from textwrap import dedent
from types import FunctionType

try:
    from functools import partial
except ImportError:
    def partial(func, *args, **keywords):
        def newfunc(*fargs, **fkeywords):
            newkeywords = keywords.copy()
            newkeywords.update(fkeywords)
            return func(*(args + fargs), **newkeywords)
        newfunc.func = func
        newfunc.args = args
        newfunc.keywords = keywords
        return newfunc

def maybe_extract_source(fun):
    if isinstance(fun, FunctionType):
        return dedent(getsource(fun))
    elif isinstance(fun, basestring):
        return fun
    raise TypeError('Function object or source string expected, got %r' % fun)

def wrap_func_to_ddoc(id, path, fun):
    _ = ddoc = {'_id': id}
    assert path[0] != '_id'
    for item in path[:-1]:
        _[item] = {}
        _ = _[item]
    _[path[-1]] = maybe_extract_source(fun)
    return ddoc
