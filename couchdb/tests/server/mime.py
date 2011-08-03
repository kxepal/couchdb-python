# -*- coding: utf-8 -*-
#
import unittest
from couchdb.server import exceptions
from couchdb.server import mime


class MimeTestCase(unittest.TestCase):

    def setUp(self):
        mime.reset_provides()
        self._kbm = mime.keys_by_mime.copy()
        self._mbk = mime.mimes_by_key.copy()

    def tearDown(self):
        mime.keys_by_mime = self._kbm.copy()
        mime.mimes_by_key = self._mbk.copy()


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
        mime.register_type('foo', 'x-foo/bar', 'x-foo/baz')
        self.assertTrue('foo' in mime.mimes_by_key)
        self.assertEqual(mime.mimes_by_key['foo'], ('x-foo/bar', 'x-foo/baz'))
        self.assertTrue('x-foo/bar' in mime.keys_by_mime)
        self.assertEqual(mime.keys_by_mime['x-foo/bar'], 'foo')
        self.assertTrue('x-foo/baz' in mime.keys_by_mime)
        self.assertEqual(mime.keys_by_mime['x-foo/baz'], 'foo')


class ProvidesTestCase(MimeTestCase):

    def test_run_first_registered_for_unknown_mimetype(self):
        """should run first provider if multiple specified"""
        def foo():
            return 'foo'
        def bar():
            return 'bar'
        mime.provides('foo', foo)
        mime.provides('bar', bar)
        self.assertEqual(mime.run_provides({}), 'foo')

    def test_fail_for_unknown_mimetype(self):
        """should raise Error if there is no information about mimetype and
        registered providers"""
        try:
            mime.run_provides({})
        except Exception, err:
            self.assertTrue(isinstance(err, exceptions.Error))
            self.assertEqual(err.args[0], 'not_acceptable')

    def test_provides_for_custom_mime(self):
        """should provides result of registered function for custom mime"""
        def foo():
            return 'foo'
        mime.provides('foo', foo)
        mime.register_type('foo', 'x-foo/bar', 'x-foo/baz')
        self.assertEqual(
            mime.run_provides({'headers': {'Accept': 'x-foo/bar'}}),
            'foo'
        )
        self.assertEqual(
            mime.run_provides({'headers': {'Accept': 'x-foo/baz'}}),
            'foo'
        )

    def test_provides_registered_mime(self):
        """should provides registered function for base mime by Accept header"""
        mime.provides('html', lambda: 'html')
        self.assertEqual(
            mime.run_provides({'headers': {'Accept': 'text/html'}}),
            'html'
        )

    def test_provides_by_query_format(self):
        """should provides registered function for base mime by query param"""
        mime.provides('html', lambda: 'html')
        self.assertEqual(
            mime.run_provides({'query': {'format': 'html'}}),
            'html'
        )

    def test_provides_uses(self):
        """should set flag if provides uses."""
        self.assertFalse(mime.provides_used())
        mime.provides('html', lambda: 'html')
        self.assertTrue(mime.provides_used())

    def test_missed_mime_key_from_accept_header(self):
        """should raise Error exception if nothing provides"""
        self.assertRaises(
            exceptions.Error,
            mime.run_provides,
            {'headers': {'Accept': 'x-foo/bar'}}
        )

    def test_missed_mime_key_from_query_format(self):
        """should raise Error exception if nothing provides"""
        self.assertRaises(
            exceptions.Error,
            mime.run_provides,
            {'query': {'format': 'foo'}}
        )

    def test_default_mimes(self):
        """should have default registered mimes"""
        self.assertEqual(
            sorted(mime.mimes_by_key.keys()),
            sorted(['all', 'atom', 'css', 'csv', 'html', 'ics', 'js', 'json',
             'multipart_form', 'rss', 'text', 'url_encoded_form', 'xhtml',
             'xml', 'yaml'])
        )

    def test_provides(self):
        """should provides new handler"""
        def foo():
            return 'foo'
        mime.provides('foo', foo)
        self.assertTrue('foo' in mime.funcs_by_key)

    def test_reset_provides(self):
        """should reset all provides"""
        def foo():
            return 'foo'
        self.assertEqual(mime.funcs_by_key, {})
        mime.provides('foo', foo)
        self.assertEqual(mime.funcs_by_key, {'foo': foo})
        mime.reset_provides()
        self.assertEqual(mime.funcs_by_key, {})


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(MimeToolsTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ProvidesTestCase, 'test'))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
