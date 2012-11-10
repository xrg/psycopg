#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from distutils.util import get_platform
import sys

platlib = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'build',
                       'lib.%s-%s' % (get_platform(), sys.version[0:3]))

if os.path.exists(platlib):
    print "Get psycopg2 from ", platlib
    sys.path.insert(0, platlib)

import psycopg2

from psycopg2.extensions import AsIs, Float
from psycopg2.extensions import ISQLParam

import uuid
import psycopg2.extras
psycopg2.extras.register_uuid()
    
from psycopg2.extensions import adapt

print psycopg2.extensions.adapters

u = uuid.UUID('9c6d5a77-7256-457e-9461-347b4358e350')
a1 = adapt(u)
print "ISQLQuote adapt:", type(a1), a1

a2 = adapt(u, ISQLParam)
print "ISQLParam adapt:", type(a2), a2
#eof
