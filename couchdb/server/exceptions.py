# -*- coding: utf-8 -*-
#

class ViewServerException(Exception):
    '''Base query server exception'''

class Error(ViewServerException):
    '''Non fatal error which doesn't initiate query server termitation.'''

class FatalError(ViewServerException):
    '''Fatal error which termitates query server.'''

class Forbidden(ViewServerException):
    '''Non fatal error which signs operation access deny.'''
