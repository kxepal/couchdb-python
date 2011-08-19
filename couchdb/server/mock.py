# -*- coding: utf-8 -*-
#
from cStringIO import StringIO
from collections import deque
from couchdb.server import BaseQueryServer

class MockQueryServer(BaseQueryServer):
    """Mock version of Python query server."""
    def __init__(self, *args, **kwargs):
        self._m_input = deque()
        self._m_output = StringIO()
        kwargs.setdefault('input', self._m_input)
        kwargs.setdefault('output', self._m_output)
        BaseQueryServer.__init__(self, *args, **kwargs)
