# -*- coding: utf-8 -*-
#
import logging
from couchdb.server.exceptions import Error

log = logging.getLogger(__name__)

__all__ = ['best_match',
           'mimes_by_key', 'keys_by_mime', 'resp_content_type',
           'provides_used', 'reset_provides',
           'register_type', 'provides', 'run_provides']

################################################################################
# Mimeparse

def parse_mimetype(mimetype):
    parts = mimetype.split(';')
    params = dict([item.split('=', 2) for item in parts if '=' in item])
    fulltype = parts[0].strip()
    if fulltype == '*':
        fulltype = '*/*'
    typeparts = fulltype.split('/')
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
            match_count = sum((1 for k, v in base_params.items()
                              if k != 'q' and params.get(k) == v))
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
    return weighted[-1][0][1] and weighted[-1][2] or ''


################################################################################
# Mime

mimes_by_key = {}
keys_by_mime = {}
funcs_by_key = {}
resp_content_type = None


def provides_used():
    return bool(funcs_by_key)

def reset_provides():
    global resp_content_type
    resp_content_type = None
    funcs_by_key.clear()

def register_type(key, *args):
    '''Register mimetypes.

    :param key: Shorthand key for mimetypes. Actually you would like
        to use extension name associated with mime types e.g. js
    :param args: full quality names of mime types.

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
    '''
    mimes_by_key[key] = args
    for item in args:
        keys_by_mime[item] = key

def provides(key, func):
    '''Register mimetype handler which will be called when design function would
    be requested with matched Content-Type value.

    :param key: Mimetype.
    :param func: Function object or any callable.
    :type key: basestring
    :type key: function or callable
    '''
    funcs_by_key[key] = func

def run_provides(req):
    global resp_content_type
    bestfun = None
    bestkey = None
    accept = None
    if 'headers' in req:
        accept = req['headers'].get('Accept')
    if 'query' in req and 'format' in req['query']:
        bestkey = req['query']['format']
        resp_content_type = mimes_by_key[bestkey][0]
    elif accept:
        supported_mimes = (mime
                   for key in funcs_by_key
                   for mime in mimes_by_key[key]
                   if key in mimes_by_key)
        resp_content_type = best_match(supported_mimes, accept)
        bestkey = keys_by_mime.get(resp_content_type)
    else:
        bestkey = funcs_by_key and funcs_by_key.keys()[0] or None
    if bestkey is not None:
        bestfun = funcs_by_key.get(bestkey)
    if bestfun is not None:
        return bestfun()
    supported_types = [', '.join(value) or key
                       for key, value in mimes_by_key.items()]
    raise Error('not_acceptable',
                'Content-Type %s not supported, try one of:\n'
                '%s' % (accept or bestkey, ', '.join(supported_types)))

# Some default types
# Ported from Ruby on Rails
# Build list of Mime types for HTTP responses
# http://www.iana.org/assignments/media-types/
# http://dev.rubyonrails.org/svn/rails/trunk/actionpack/lib/action_controller/mime_types.rb
types = {
    'all': ['*/*'],
    'text': ['text/plain; charset=utf-8', 'txt'],
    'html': ['text/html; charset=utf-8'],
    'xhtml': ['application/xhtml+xml', 'xhtml'],
    'xml': ['application/xml', 'text/xml', 'application/x-xml'],
    'js': ['text/javascript', 'application/javascript', 'application/x-javascript'],
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
}
for k, v in types.items():
    register_type(k, *v)
