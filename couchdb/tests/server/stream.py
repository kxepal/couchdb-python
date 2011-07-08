# -*- coding: utf-8 -*-
#
import unittest
from StringIO import StringIO
from couchdb.server import exceptions
from couchdb.server import stream
from couchdb.server import state


class StreamTestCase(unittest.TestCase):

    def setUp(self):
        self._input = stream.input
        self._output = stream.output

    def tearDown(self):
        stream.input = self._input
        stream.output = self._output
        state.line_length = 0

    def test_receive(self):
        """should decode json data from input stream"""
        stream.input = StringIO('["foo", "bar"]\n["bar", {"foo": "baz"}]')
        reader = stream.receive()
        self.assertEqual(reader.next(), ['foo', 'bar'])
        self.assertEqual(reader.next(), ['bar', {'foo': 'baz'}])
        self.assertRaises(StopIteration, reader.next)

    def test_update_last_line_length(self):
        """should update last line length info"""
        stream.input = StringIO('["foo", "bar"]\n["bar", {"foo": "baz"}]')
        reader = stream.receive()
        reader.next()
        self.assertEqual(state.line_length, len('["foo", "bar"]\n'))

    def test_fail_on_receive_invalid_json_data(self):
        """should raise FatalError if json decode fails"""
        stream.input = StringIO('["foo", "bar" "bar", {"foo": "baz"}]')
        try:
            stream.receive().next()
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'json_decode')

    def test_respond(self):
        """should encode object to json and write it to output stream"""
        output = StringIO()
        stream.output = output
        stream.respond(['foo', {'bar': ['baz']}])
        self.assertEqual(output.getvalue(), '["foo", {"bar": ["baz"]}]\n')

    def test_fail_on_respond_unserializable_to_json_object(self):
        """should raise FatalError if json encode fails"""
        stream.output = StringIO()
        try:
            stream.respond(['error', 'foo', IOError('bar')])
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'json_encode')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(StreamTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
