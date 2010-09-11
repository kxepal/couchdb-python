# -*- coding: utf-8 -*-
import sys

class CouchDBVersion(object):
    current = ('?', '?', '?')

    @classmethod
    def _wrapper(cls, minver, _memory={}):
        ''' Simple decorator which allows to call specific function depending on
            information of minimal and maximal version.
        '''
        def memorize(fun):
            def decorator(*args, **kwargs):
                errstr = 'There is no function for version %s' % (cls.current,)
                fname = fun.func_name
                decid = id(decorator)
                for fname, decset in _memory.items():
                    if fname == fun.func_name and decid in decset:
                        break
                else:
                    # actually, I couldn't reproduce this case, but I believe
                    # it exists
                    raise NotImplementedError(errstr)
                pair = ((0, 0, 0), None)
                for ver, func in decset[decid].items():
                    if cls.current >= ver >= pair[0]:
                        pair = (ver, func)
                ver, func = pair
                if func is None:
                    raise NotImplementedError(errstr)
                return func(*args, **kwargs)
            decfun = None
            fname = fun.func_name
            outer_scope = sys._getframe(1).f_locals
            if fname in outer_scope:
                decfun = outer_scope[fname]
            if not fname in _memory:
                _memory[fname] = {}
            decid = decfun is None and id(decorator) or id(decfun)
            if not decid in _memory[fname]:
                _memory[fname][decid] = {}
            _memory[fname][decid][minver] = fun
            decorator.__doc__ = fun.__doc__
            return decfun or decorator
        return memorize

    @classmethod
    def minimal(cls, major=0, minor=0, micro=0):
        return cls._wrapper((major, minor, micro))