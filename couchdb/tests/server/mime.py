# -*- coding: utf-8 -*-
#
import unittest
from couchdb.server import exceptions
from couchdb.server import mime


class MimeTestCase(unittest.TestCase):

    def setUp(self):
        self.provider = mime.MimeProvider()

class MimeToolsTestCase(MimeTestCase):

    def test_best_match(self):
        """should match mime"""
        self.assertEqual(
            mime.best_match(['application/json', 'text/x-json'],
                            'application/json'),
            "application/json"
        )

    def test_best_match_is_nothing(self):
        """should return empty string if nothing matched"""
        self.assertEqual(
            mime.best_match(['application/json', 'text/x-json'], 'x-foo/bar'),
            ''
        )

    def test_best_match_by_quality(self):
        """should return match mime with best quality"""
        self.assertEqual(
            mime.best_match(['application/json', 'text/x-json'],
                            'text/x-json;q=1'),
            'text/x-json'
        )

    def test_best_match_by_wildcard(self):
        """should match mimetype by wildcard"""
        self.assertEqual(
            mime.best_match(['application/json', 'text/x-json'],
                            'application/*'),
            'application/json'
        )

    def test_best_match_prefered_direct_match(self):
        """should match by direct hit"""
        self.assertEqual(
            mime.best_match(['application/json', 'text/x-json'],
                            '*/*,application/json,*'),
            'application/json'
        )

    def test_best_match_supports_nothing(self):
        """should return empty string if nothing could be matched"""
        self.assertEqual(mime.best_match([], 'text/html'), '')

    def test_register_type(self):
        """should register multiple mimetypes for single keyword"""
        self.provider.register_type('foo', 'x-foo/bar', 'x-foo/baz')
        self.assertTrue('foo' in self.provider.mimes_by_key)
        self.assertEqual(self.provider.mimes_by_key['foo'], ('x-foo/bar', 'x-foo/baz'))
        self.assertTrue('x-foo/bar' in self.provider.keys_by_mime)
        self.assertEqual(self.provider.keys_by_mime['x-foo/bar'], 'foo')
        self.assertTrue('x-foo/baz' in self.provider.keys_by_mime)
        self.assertEqual(self.provider.keys_by_mime['x-foo/baz'], 'foo')

    def test_parse_malformed_mimetype(self):
        """should not raise IndexError exception if MIME type is invalid"""
        mime.parse_mimetype('text')
        mime.parse_mimetype('')


class ProvidesTestCase(MimeTestCase):

    def test_run_first_registered_for_unknown_mimetype(self):
        """should run first provider if multiple specified"""
        def foo():
            return 'foo'
        def bar():
            return 'bar'
        self.provider.provides('foo', foo)
        self.provider.provides('bar', bar)
        self.assertEqual(self.provider.run_provides({}), 'foo')

    def test_fail_for_unknown_mimetype(self):
        """should raise Error if there is no information about mimetype and
        registered providers"""
        try:
            self.provider.run_provides({})
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.Error))
            self.assertEqual(err.args[0], 'not_acceptable')

    def test_provides_for_custom_mime(self):
        """should provides result of registered function for custom mime"""
        def foo():
            return 'foo'
        self.provider.provides('foo', foo)
        self.provider.register_type('foo', 'x-foo/bar', 'x-foo/baz')
        self.assertEqual(
            self.provider.run_provides({'headers': {'Accept': 'x-foo/bar'}}),
            'foo'
        )
        self.assertEqual(
            self.provider.run_provides({'headers': {'Accept': 'x-foo/baz'}}),
            'foo'
        )

    def test_provides_registered_mime(self):
        """should provides registered function for base mime by Accept header"""
        self.provider.provides('html', lambda: 'html')
        self.assertEqual(
            self.provider.run_provides({'headers': {'Accept': 'text/html'}}),
            'html'
        )

    def test_provides_by_query_format(self):
        """should provides registered function for base mime by query param"""
        self.provider.provides('html', lambda: 'html')
        self.assertEqual(
            self.provider.run_provides({'query': {'format': 'html'}}),
            'html'
        )

    def test_provides_uses(self):
        """should set flag if provides uses."""
        self.assertFalse(self.provider.is_provides_used())
        self.provider.provides('html', lambda: 'html')
        self.assertTrue(self.provider.is_provides_used())

    def test_missed_mime_key_from_accept_header(self):
        """should raise Error exception if nothing provides"""
        self.assertRaises(
            exceptions.Error,
            self.provider.run_provides,
            {'headers': {'Accept': 'x-foo/bar'}}
        )

    def test_missed_mime_key_from_query_format(self):
        """should raise Error exception if nothing provides"""
        self.assertRaises(
            exceptions.Error,
            self.provider.run_provides,
            {'query': {'format': 'foo'}}
        )

    def test_default_mimes(self):
        """should have default registered mimes"""
        self.assertEqual(
            sorted(self.provider.mimes_by_key.keys()),
            sorted(['all', 'atom', 'css', 'csv', 'html', 'ics', 'js', 'json',
             'multipart_form', 'rss', 'text', 'url_encoded_form', 'xhtml',
             'xml', 'yaml'])
        )

    def test_provides(self):
        """should provides new handler"""
        def foo():
            return 'foo'
        self.provider.provides('foo', foo)
        self.assertTrue('foo' in self.provider.funcs_by_key)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(MimeToolsTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ProvidesTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
