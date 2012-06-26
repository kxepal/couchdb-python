# -*- coding: utf-8 -*-
#
from collections import deque
from couchdb import json
from couchdb.server import SimpleQueryServer

class MockStream(deque):

    def readline(self):
        if self:
            return self.popleft()
        else:
            return ''

    def write(self, data):
        if isinstance(data, basestring):
            self.append(json.decode(data))
        else:
            self.append(data)

    def flush(self):
        pass

class MockQueryServer(SimpleQueryServer):
    """Mock version of Python query server."""
    def __init__(self, *args, **kwargs):
        self._m_input = MockStream()
        self._m_output = MockStream()
        kwargs.setdefault('input', self._m_input)
        kwargs.setdefault('output', self._m_output)
        super(MockQueryServer, self).__init__(*args, **kwargs)

    def m_input_write(self, data):
        self._m_input.append(json.encode(data))

    def m_output_read(self):
        output = self._m_output
        return [output.popleft() for i in range(len(output)) if output]
