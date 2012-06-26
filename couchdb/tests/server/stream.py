# -*- coding: utf-8 -*-
#
import unittest
from StringIO import StringIO
from couchdb.server import exceptions
from couchdb.server import stream


class StreamTestCase(unittest.TestCase):

    def test_receive(self):
        """should decode json data from input stream"""
        input = StringIO('["foo", "bar"]\n["bar", {"foo": "baz"}]')
        reader = stream.receive(input)
        self.assertEqual(reader.next(), ['foo', 'bar'])
        self.assertEqual(reader.next(), ['bar', {'foo': 'baz'}])
        self.assertRaises(StopIteration, reader.next)

    def test_fail_on_receive_invalid_json_data(self):
        """should raise FatalError if json decode fails"""
        input = StringIO('["foo", "bar" "bar", {"foo": "baz"}]')
        try:
            stream.receive(input).next()
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'json_decode')

    def test_respond(self):
        """should encode object to json and write it to output stream"""
        output = StringIO()
        stream.respond(['foo', {'bar': ['baz']}], output)
        self.assertEqual(output.getvalue(), '["foo", {"bar": ["baz"]}]\n')

    def test_fail_on_respond_unserializable_to_json_object(self):
        """should raise FatalError if json encode fails"""
        output = StringIO()
        try:
            stream.respond(['error', 'foo', IOError('bar')], output)
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.FatalError))
            self.assertEqual(err.args[0], 'json_encode')

    def test_respond_none(self):
        """should not send any data if None passed"""
        output = StringIO()
        stream.respond(None, output)
        self.assertEqual(output.getvalue(), '')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(StreamTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
