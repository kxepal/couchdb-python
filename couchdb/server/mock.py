# -*- coding: utf-8 -*-
#
from couchdb import json
from cStringIO import StringIO
from collections import deque
from couchdb.server import SimpleQueryServer

class MockQueryServer(SimpleQueryServer):
    """Mock version of Python query server."""
    def __init__(self, *args, **kwargs):
        self._m_input = deque()
        self._m_output = StringIO()
        kwargs.setdefault('input', self._m_input)
        kwargs.setdefault('output', self._m_output)
        super(MockQueryServer, self).__init__(*args, **kwargs)

    def m_input_write(self, data):
        self._m_input.append(json.encode(data))

    def m_output_read(self):
        res = []
        for line in StringIO(self._m_output.getvalue()):
            res.append(json.decode(line))
        return res

