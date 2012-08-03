# -*- coding: utf-8 -*-
#
import logging
from pprint import pformat
from couchdb.server.exceptions import Error

log = logging.getLogger(__name__)

__all__ = ['best_match', 'MimeProvider', 'DEFAULT_TYPES']

def parse_mimetype(mimetype):
    parts = mimetype.split(';')
    params = {}
    for item in parts[1:]:
        if '=' in item:
            key, value = item.split('=', 2)
        else:
            key, value = item, None
        params[key] = value
    fulltype = parts[0].strip()
    if fulltype == '*':
        fulltype = '*/*'
    if '/' in fulltype:
        typeparts = fulltype.split('/', 2)
    else:
        typeparts = fulltype, None
    return typeparts[0], typeparts[1], params

def parse_media_range(range):
    parsed_type = parse_mimetype(range)
    q = float(parsed_type[2].get('q', '1'))
    if q < 0 or q >= 1:
        parsed_type[2]['q'] = '1'
    return parsed_type

def fitness_and_quality(mimetype, ranges):
    parsed_ranges = [parse_media_range(item) for item in ranges.split(',')]
    best_fitness = -1
    best_fit_q = 0
    base_type, base_subtype, base_params = parse_media_range(mimetype)
    for parsed in parsed_ranges:
        type, subtype, params = parsed
        type_preq = type == base_type or '*' in [type, base_type]
        subtype_preq = subtype == base_subtype or '*' in [subtype, base_subtype]
        if type_preq and subtype_preq:
            match_count = sum(
                1 for k, v in base_params.items()
                if k != 'q' and params.get(k) == v)
            fitness = type == base_type and 100 or 0
            fitness += subtype == base_subtype and 10 or 0
            fitness += match_count
            if fitness > best_fitness:
                best_fitness = fitness
                best_fit_q = params.get('q', 0)
    return best_fitness, float(best_fit_q)

def quality(mimetype, ranges):
    return fitness_and_quality(mimetype, ranges)

def best_match(supported, header):
    weighted = []
    for i, item in enumerate(supported):
        weighted.append([fitness_and_quality(item, header), i, item])
    weighted.sort()
    log.debug('Best match rating, last wins:\n%s', pformat(weighted))
    return weighted and weighted[-1][0][1] and weighted[-1][2] or ''


#: Some default types.
#: Build list of `MIME types <http://www.iana.org/assignments/media-types/>`_ for HTTP responses.
#: Ported from `Ruby on Rails <https://github.com/rails/rails/blob/v3.1.0/actionpack/lib/action_dispatch/http/mime_types.rb>`_
DEFAULT_TYPES = {
    'all': ['*/*'],
    'text': ['text/plain; charset=utf-8', 'txt'],
    'html': ['text/html; charset=utf-8'],
    'xhtml': ['application/xhtml+xml', 'xhtml'],
    'xml': ['application/xml', 'text/xml', 'application/x-xml'],
    'js': ['text/javascript', 'application/javascript',
           'application/x-javascript'],
    'css': ['text/css'],
    'ics': ['text/calendar'],
    'csv': ['text/csv'],
    'rss': ['application/rss+xml'],
    'atom': ['application/atom+xml'],
    'yaml': ['application/x-yaml', 'text/yaml'],
    # just like Rails
    'multipart_form': ['multipart/form-data'],
    'url_encoded_form': ['application/x-www-form-urlencoded'],
    # http://www.ietf.org/rfc/rfc4627.txt
    'json': ['application/json', 'text/x-json']
    # TODO: https://issues.apache.org/jira/browse/COUCHDB-1261
    # 'kml', 'application/vnd.google-earth.kml+xml',
    # 'kmz', 'application/vnd.google-earth.kmz'
}


class MimeProvider(object):
    """Provides custom function depending on requested MIME type."""

    def __init__(self):
        self.mimes_by_key = {}
        self.keys_by_mime = {}
        self.funcs_by_key = {}
        self._resp_content_type = None

        for k, v in DEFAULT_TYPES.items():
            self.register_type(k, *v)

    def is_provides_used(self):
        """Checks if any provides function is registered."""
        return bool(self.funcs_by_key)

    @property
    def resp_content_type(self):
        """Returns actual response content type."""
        return self._resp_content_type

    def register_type(self, key, *args):
        """Register MIME types.

        :param key: Shorthand key for list of MIME types.
        :type key: str

        :param args: List of full quality names of MIME types.

        Predefined types:
            - all: ``*/*``
            - text: ``text/plain; charset=utf-8``, ``txt``
            - html: ``text/html; charset=utf-8``
            - xhtml: ``application/xhtml+xml``, ``xhtml``
            - xml: ``application/xml``, ``text/xml``, ``application/x-xml``
            - js: ``text/javascript``, ``application/javascript``,
              ``application/x-javascript``
            - css: ``text/css``
            - ics: ``text/calendar``
            - csv: ``text/csv``
            - rss: ``application/rss+xml``
            - atom: ``application/atom+xml``
            - yaml: ``application/x-yaml``, ``text/yaml``
            - multipart_form: ``multipart/form-data``
            - url_encoded_form: ``application/x-www-form-urlencoded``
            - json: ``application/json``, ``text/x-json``

        Example:
            >>> register_type('png', 'image/png')
        """
        self.mimes_by_key[key] = args
        for item in args:
            self.keys_by_mime[item] = key

    def provides(self, key, func):
        """Register MIME type handler which will be called when design function
        would be requested with matched `Content-Type` value.

        :param key: MIME type.
        :type key: str

        :param func: Function object or any callable.
        :type func: function or callable
        """
        # TODO: https://issues.apache.org/jira/browse/COUCHDB-898
        self.funcs_by_key[key] = func

    def run_provides(self, req, default=None):
        bestfun = None
        bestkey = None
        accept = None
        if 'headers' in req:
            accept = req['headers'].get('Accept')
        if 'query' in req and 'format' in req['query']:
            bestkey = req['query']['format']
            if bestkey in self.mimes_by_key:
                self._resp_content_type = self.mimes_by_key[bestkey][0]
        elif accept:
            supported_mimes = (mime
               for key in self.funcs_by_key
               for mime in self.mimes_by_key[key]
               if key in self.mimes_by_key)
            self._resp_content_type = best_match(supported_mimes, accept)
            bestkey = self.keys_by_mime.get(self._resp_content_type)
        else:
            bestkey = self.funcs_by_key and self.funcs_by_key.keys()[0] or None
        log.debug('Provides\nBest key: %s\nBest mime: %s\nRequest: %s',
                  bestkey, self.resp_content_type, req)
        if bestkey is not None:
            bestfun = self.funcs_by_key.get(bestkey)
        if bestfun is not None:
            return bestfun()
        if default is not None and default in self.funcs_by_key:
            bestkey = default
            bestfun = self.funcs_by_key[default]
            self._resp_content_type = self.mimes_by_key[default][0]
            log.debug('Provides fallback\n'
                      'Best key: %s\nBest mime: %s\nRequest: %s',
                      bestkey, self.resp_content_type, req)
            return bestfun()
        supported_types = ', '.join(
            ', '.join(value) or key for key, value in self.mimes_by_key.items())
        content_type = accept or self.resp_content_type or bestkey
        msg = 'Content-Type %s not supported, try one of:\n%s'
        log.error(msg, content_type, supported_types)
        raise Error('not_acceptable', msg % (content_type, supported_types))
