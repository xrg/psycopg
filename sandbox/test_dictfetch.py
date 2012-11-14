#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from distutils.util import get_platform
import sys
from sys import stderr
from optparse import OptionParser
from operator import itemgetter

parser = OptionParser()
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

parser.add_option("-t", None,
                  action="store_true", dest="old_proto", default=False,
                  help="Uses the old (installed) library")

parser.add_option("-b", None,
                  action="store_true", dest="bin_proto", default=False,
                  help="Uses the binary cursor")


(options, args) = parser.parse_args()

# Insert the distutils build directory into the path, if it exists.
platlib = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'build',
                       'lib.%s-%s' % (get_platform(), sys.version[0:3]))
if os.path.exists(platlib) and not options.old_proto:
    print "Get psycopg2 from ", platlib
    sys.path.insert(0, platlib)
else:
    print "note: platform lib is not at ", platlib

import psycopg2
from psycopg2.psycopg1 import cursor as psycopg1cursor
from psycopg2.extras import DictCursor

if options.bin_proto:
    cursorKlass = psycopg2.extensions.cursor_bin
    print "Using a binary cursor"
else:
    cursorKlass = psycopg2.extensions.cursor

#from psycopg2._psycopg import AsIs, Float

dbname = os.environ.get('PSYCOPG2_TESTDB', 'test_bqi')
dbhost = os.environ.get('PSYCOPG2_TESTDB_HOST', None)
dbport = os.environ.get('PSYCOPG2_TESTDB_PORT', None)
dbuser = os.environ.get('PSYCOPG2_TESTDB_USER', None)

# Construct a DSN to connect to the test database:
dsn = 'dbname=%s' % dbname
if dbhost is not None:
    dsn += ' host=%s' % dbhost
if dbport is not None:
    dsn += ' port=%s' % dbport
if dbuser is not None:
    dsn += ' user=%s' % dbuser

def timed(orig_func):
    def f(*args, **kwargs):
        ost_begin = os.times()
        ret = orig_func(*args, **kwargs)
        ost_end = os.times()
        ost_diff = []
        for n, x in enumerate(ost_begin):
            ost_diff.append(ost_end[n] - x)
        print >>stderr, "Times: %.3f %.3f %.3f %.3f %.3f" % tuple(ost_diff)
    
    return f

class PyDictCursor(cursorKlass):
    def __build_cols(self):
        return map(itemgetter(0), self.description)

    def dictfetchall(self):
        rows = self.fetchall()
        cols = self.__build_cols()
        return [ dict(zip(cols, row)) for row in rows]

class Dict1Cursor(cursorKlass):
    def dictfetchall(self):
        self.row_factory = lambda s: dict()
        ret = self.fetchall()
        self.row_factory = None
        return ret

class Dict2Cursor(cursorKlass):
    def dictfetchall(self):
        self.row_factory = lambda s: {}
        ret = self.fetchall()
        self.row_factory = None
        return ret

class Dict3Cursor(cursorKlass):
    def dictfetchall(self):
        self.row_factory = dict
        ret = self.fetchall()
        self.row_factory = None
        return ret

#@timed
def do_query(cr):
    cr.execute('SELECT * FROM ir_model_data')


@timed
def fetch_data_list(cr):
    data = cr.fetchall()
    print "Data len: %d" % len(data)

@timed
def fetch_data(cr):
    data = cr.dictfetchall()
    print "Data len: %d" % len(data)

#@timed
#def fetch_data(cr):
    #data = cr.dictfetchall()


conn = psycopg2.connect(dsn)
print 'Connection established'


print "List case:"
cr = conn.cursor()
do_query(cr)
fetch_data_list(cr)
cr.close()
print

print "Dictfetchall case with psygopg1:"
cr = psycopg1cursor(conn)
do_query(cr)
fetch_data(cr)
cr.close()
print

print "Dictfetchall case with py-cursor:"
cr = PyDictCursor(conn)
do_query(cr)
fetch_data(cr)
cr.close()
print

print "Dictfetchall case with DictCursor:"
cr = DictCursor(conn)
do_query(cr)
fetch_data_list(cr)
cr.close()
print

print "Dictfetchall case with implicit dict cursor:"
cr = Dict1Cursor(conn)
do_query(cr)
fetch_data(cr)
cr.close()
print

print "Dictfetchall case with implicit dict 2 cursor:"
cr = Dict2Cursor(conn)
do_query(cr)
fetch_data(cr)
cr.close()
print

print "Dictfetchall case with implicit dict 2 cursor:"
cr = Dict3Cursor(conn)
do_query(cr)
fetch_data(cr)
cr.close()
print


#eof
