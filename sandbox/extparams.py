#!/usr/bin/env python

import os

from distutils.util import get_platform
import sys

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

parser.add_option("-t", None,
                  action="store_true", dest="old_proto", default=False,
                  help="Uses the old (installed) library")

parser.add_option("-n", "--num-reps", dest="num_reps", 
		  help="Number of repetitions for stress test",)

parser.add_option("-N", "--no-stress",
                  action="store_false", dest="stress", default=True,
                  help="Disable the stress test")

parser.add_option("-R", "--no-regulars",
                  action="store_false", dest="regulars", default=True,
                  help="Disable the regular tests")


(options, args) = parser.parse_args()

# Insert the distutils build directory into the path, if it exists.
platlib = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'build',
                       'lib.%s-%s' % (get_platform(), sys.version[0:3]))
if os.path.exists(platlib) and not options.old_proto:
    print "Get psycopg2 from ", platlib
    sys.path.insert(0, platlib)

import psycopg2
from psycopg2._psycopg import AsIs

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

if options.regulars:
    print "Simple select:"
    cr.execute('SELECT %s',(1,))
    print cr.fetchall()
    
    print "Single cmd with semicolon:"
    cr.execute("SELECT %s;   	", (1,))
    print cr.fetchall()

    def testUUIDARRAY():
	import uuid
	import psycopg2.extras
	psycopg2.extras.register_uuid()
        u = [uuid.UUID('9c6d5a77-7256-457e-9461-347b4358e350'), uuid.UUID('9c6d5a77-7256-457e-9461-347b4358e352')]
        cr.execute("SELECT %s AS foo", (u,))
        
        print cr.fetchall()

    try:
        print "Multi cmds:"
        cr.execute("SELECT '%s'; SELECT '2' ;", (1,))
        print cr.fetchall()
    except psycopg2.ProgrammingError, e:
        print "Programming error:", e
        pass
    
    args = (1, 1L, -1, 'str1', True, False, u'Δοκιμή', 0.123)
    args = (1, 1L, -1, 'str1', True, False, u'Δοκιμή')
    
    qry = 'SELECT ' + ', '.join('%s' * len(args)) + ';'
    cr.execute(qry, args)
    print "Result:", cr.fetchall()
    
    #cr.execute('SELECT %s::TEXT, %s::INTEGER, %s::TEXT, %s::INTERVAL ;',
    #        (None, AsIs(1), AsIs('NULL'), AsIs("'1 hour'")))
    # print "Result 2:", cr.fetchall()
    
    cr.execute('SELECT %s', ([ 1, 2, 3, 4 ],))
    print "Result 3:", cr.fetchall()
    
    testUUIDARRAY()

if options.stress:
    ran = []
    for r in range(1, 500):
	ran.extend([r, 0.4* r, u'αβγ' + str(r)])
	# ran.extend([r * 0.1, r* 0.15, r* 0.3])
    print "trying with: ", ran[:5]
    qry = 'SELECT (' + ', '.join([ '%s' for x in ran]) + ');'
    for i in range(1, 1000):
        cr.execute(qry, ran)
        res = cr.fetchall()

