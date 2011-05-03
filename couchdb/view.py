#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2008 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Implementation of a view server for functions written in Python."""
import os
import sys

from couchdb import json
from couchdb.server import construct_server

__all__ = ['main', 'run']
__docformat__ = 'restructuredtext en'

def run(input=sys.stdin, output=sys.stdout, version=None, **config):
    qs = construct_server(version, **config)
    return qs.run(input, output)

_VERSION = """%(name)s - CouchDB Python %(version)s

Copyright (C) 2007 Christopher Lenz <cmlenz@gmx.de>.
"""

_HELP = """Usage: %(name)s [OPTION]

The %(name)s command runs the CouchDB Python view server.

The exit status is 0 for success or 1 for failure.

Options:

  --version               display version information and exit
  -h, --help              display a short help message and exit
  --json-module=<name>    set the JSON module to use ('simplejson', 'cjson',
                          or 'json' are supported)
  --log-file=<file>       log file path.
  --log-level=<level>     specify logging level (debug, info, warn, error).
                          Used info level if omitted.
  --couchdb-version=<ver> define with which version of couchdb server will work
                          default: latest implemented.
                          Supports from 0.9.0 to 1.1.0 and trunk. Technicaly
                          should work with 0.8.0.
                          e.g.: --couchdb-version=0.9.0

Report bugs via the web at <http://code.google.com/p/couchdb-python>.
"""

def main():
    """Command-line entry point for running the view server."""
    import getopt
    from couchdb import __version__ as VERSION
    qs_config = {}
    try:
        option_list, argument_list = getopt.gnu_getopt(
            sys.argv[1:], 'h',
            ['version', 'help', 'json-module=', 'log-level=', 'log-file=',
             'couchdb-version=']
        )
        version = None
        message = None
        for option, value in option_list:
            if option in ['--version']:
                message = _VERSION % dict(name=os.path.basename(sys.argv[0]),
                                      version=VERSION)
            elif option in ['-h', '--help']:
                message = _HELP % dict(name=os.path.basename(sys.argv[0]))
            elif option in ['--json-module']:
                json.use(module=value)
            elif option in ['--log-level']:
                qs_config['log_level'] = value.upper()
            elif option in ['--log-file']:
                qs_config['log_file'] = value
            elif option in ['--couchdb-version']:
                if value.lower() != 'trunk':
                    version = value.split('.')
                    while len(version) < 3:
                        version.append(0)
                    version = tuple(map(int, version[:3]))
        if message:
            sys.stdout.write(message)
            sys.stdout.flush()
            sys.exit(0)

    except getopt.GetoptError, error:
        message = '%s\n\nTry `%s --help` for more information.\n' % (
            str(error), os.path.basename(sys.argv[0])
        )
        sys.stderr.write(message)
        sys.stderr.flush()
        sys.exit(1)
    sys.exit(run(version=version, **qs_config))


if __name__ == '__main__':
    main()
