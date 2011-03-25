# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2009 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

import random
import sys
import subprocess
import types
import unittest
from couchdb import client
from couchdb import json

class TempDatabaseMixin(object):

    temp_dbs = None
    _db = None

    def setUp(self):
        self.server = client.Server(full_commit=False)

    def tearDown(self):
        if self.temp_dbs:
            for name in self.temp_dbs:
                self.server.delete(name)

    def temp_db(self):
        if self.temp_dbs is None:
            self.temp_dbs = {}
        # Find an unused database name
        while True:
            name = 'couchdb-python/%d' % random.randint(0, sys.maxint)
            if name not in self.temp_dbs:
                break
            print '%s already used' % name
        db = self.server.create(name)
        self.temp_dbs[name] = db
        return name, db

    def del_db(self, name):
        del self.temp_dbs[name]
        self.server.delete(name)

    @property
    def db(self):
        if self._db is None:
            name, self._db = self.temp_db()
        return self._db


class TestRunner(unittest.main):
    def runTests(self):
        if self.testRunner is None:
            self.testRunner = unittest.TextTestRunner
        if isinstance(self.testRunner, (type, types.ClassType)):
            try:
                testRunner = self.testRunner(verbosity=self.verbosity)
            except TypeError:
                # didn't accept the verbosity, buffer or failfast arguments
                testRunner = self.testRunner()
        else:
            # it is assumed to be a TestRunner instance
            testRunner = self.testRunner
        self.result = testRunner.run(self.test)
        # remove forced exit
        #if self.exit:
        #    sys.exit(not self.result.wasSuccessful())


class QueryServer(object):

    class Reader(object):
        def __init__(self, proc, stream):
            self.proc = proc
            self.stream = stream

        def read(self):
            while not self.stream.closed and self.proc.poll() is None:
                line = self.stream.readline()
                if not line:
                    continue
                data = json.decode(line)
                if isinstance(data, (list, dict)) and 'log' in data:
                    # dont count log output in tests
                    continue
                if data is None:
                    continue
                yield data

    def __init__(self, viewsrv_path, version):
        version = '.'.join(map(str, version))
        exc = [sys.executable, viewsrv_path, '--couchdb-version=' + version]
        self.pipe = subprocess.Popen(exc, stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE)
        self.input = self.pipe.stdin
        self.output = self.pipe.stdout
        self.reader = self.Reader(self.pipe, self.output)

    def close(self):
        self.input.close()
        self.reader.stream.close()
        self.pipe.wait()
        return self.pipe.returncode

    def run(self, query):
        self.send(query)
        return self.recv()

    def send(self, query):
        self.input.write(json.encode(query) + '\n')

    def recv(self):
        for data in self.reader.read():
            return data

    def reset(self):
        return self.run(['reset'])

    def add_fun(self, fun):
        return self.run(['add_fun', fun])

    def teach_ddoc(self, ddoc):
        return self.run(['ddoc', 'new', self.ddoc_id(ddoc), ddoc])

    def ddoc_id(self, ddoc):
        d_id = ddoc.get('_id')
        assert d_id, 'document _id missed'
        return d_id

    def send_ddoc(self, ddoc, fun_path, args):
        self.send(['ddoc', self.ddoc_id(ddoc), fun_path, args])

    def ddoc_run(self, ddoc, fun_path, args):
        return self.run(['ddoc', self.ddoc_id(ddoc), fun_path, args])
