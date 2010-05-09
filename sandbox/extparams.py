#!/usr/bin/env python

import os

from distutils.util import get_platform
import sys

# Insert the distutils build directory into the path, if it exists.
platlib = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'build',
                       'lib.%s-%s' % (get_platform(), sys.version[0:3]))
if os.path.exists(platlib):
    print "Get psycopg2 from ", platlib
    sys.path.insert(0, platlib)

import psycopg2

dbname = os.environ.get('PSYCOPG2_TESTDB', 'psycopg2_test')
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


conn = psycopg2.connect(dsn)
print 'Connection established'
cr = conn.cursor()

cr.execute('SELECT %s',(1,))
print cr.fetchall()

ran = range(1, 1500)
qry = 'SELECT (' + ', '.join([ '%s' for x in ran]) + ');'
for i in range(1, 1000):
    cr.execute(qry, ran)
    res = cr.fetchall()

