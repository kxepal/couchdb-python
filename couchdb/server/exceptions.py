# -*- coding: utf-8 -*-
#

class ViewServerException(Exception):
    """Base query server exception"""

class Error(ViewServerException):
    """Non fatal error which should not terminate query serve"""

class FatalError(ViewServerException):
    """Fatal error which should terminates query server"""

class Forbidden(ViewServerException):
    """Non fatal error which signs access deny for processed operation"""
