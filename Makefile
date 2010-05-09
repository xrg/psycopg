PYTHON = python$(PYTHON_VERSION)
#PYTHON_VERSION = 2.4

TESTDB = psycopg2_test

all:
	@python ./setup.py build

check:
	@echo "* Creating $(TESTDB)"
	@if psql -l | grep -q " $(TESTDB) "; then \
	    dropdb $(TESTDB) >/dev/null; \
	fi
	createdb $(TESTDB)
	PSYCOPG2_TESTDB=$(TESTDB) $(PYTHON) runtests.py --verbose
