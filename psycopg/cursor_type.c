/* cursor_type.c - python interface to cursor objects
 *
 * Copyright (C) 2003-2010 Federico Di Gregorio <fog@debian.org>
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <string.h>

#define PSYCOPG_MODULE
#include "psycopg/config.h"
#include "psycopg/python.h"
#include "psycopg/psycopg.h"
#include "psycopg/cursor.h"
#include "psycopg/connection.h"
#include "psycopg/green.h"
#include "psycopg/pqpath.h"
#include "psycopg/typecast.h"
#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"
#include "pgversion.h"
#include <stdlib.h>

extern PyObject *pyPsycopgTzFixedOffsetTimezone;

#define HAVE_EXECPARAMS 1

/** DBAPI methods **/

/* close method - close the cursor */

#define psyco_curs_close_doc \
"close() -- Close the cursor."

static PyObject *
psyco_curs_close(cursorObject *self, PyObject *args)
{
    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, close);

    if (self->name != NULL) {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        PyOS_snprintf(buffer, 127, "CLOSE %s", self->name);
        if (pq_execute(self, buffer, 0) == -1) return NULL;
    }

    self->closed = 1;
    Dprintf("psyco_curs_close: cursor at %p closed", self);

    Py_INCREF(Py_None);
    return Py_None;
}


/* execute method - executes a query */

/* mogrify a query string and build argument array or dict */

static int
_mogrify(PyObject *var, PyObject *fmt, connectionObject *conn, PyObject **new)
{
    PyObject *key, *value, *n, *item;
    char *d, *c;
    Py_ssize_t index = 0;
    int force = 0, kind = 0;

    /* from now on we'll use n and replace its value in *new only at the end,
       just before returning. we also init *new to NULL to exit with an error
       if we can't complete the mogrification */
    n = *new = NULL;
    c = PyString_AsString(fmt);

    while(*c) {
        /* handle plain percent symbol in format string */
        if (c[0] == '%' && c[1] == '%') {
            c+=2; force = 1;
        }

        /* if we find '%(' then this is a dictionary, we:
           1/ find the matching ')' and extract the key name
           2/ locate the value in the dictionary (or return an error)
           3/ mogrify the value into something usefull (quoting)...
           4/ ...and add it to the new dictionary to be used as argument
        */
        else if (c[0] == '%' && c[1] == '(') {

            /* check if some crazy guy mixed formats */
            if (kind == 2) {
                Py_XDECREF(n);
                psyco_set_error(ProgrammingError, (PyObject*)conn,
                   "argument formats can't be mixed", NULL, NULL);
                return -1;
            }
            kind = 1;

            /* let's have d point the end of the argument */
            for (d = c + 2; *d && *d != ')'; d++);

            if (*d == ')') {
                key = PyString_FromStringAndSize(c+2, (Py_ssize_t) (d-c-2));
                value = PyObject_GetItem(var, key);
                /* key has refcnt 1, value the original value + 1 */

                /*  if value is NULL we did not find the key (or this is not a
                    dictionary): let python raise a KeyError */
                if (value == NULL) {
                    Py_DECREF(key); /* destroy key */
                    Py_XDECREF(n);  /* destroy n */
                    return -1;
                }

                Dprintf("_mogrify: value refcnt: "
                  FORMAT_CODE_PY_SSIZE_T " (+1)", value->ob_refcnt);

                if (n == NULL) {
                    n = PyDict_New();
                }

                if ((item = PyObject_GetItem(n, key)) == NULL) {
                    PyObject *t = NULL;

                    PyErr_Clear();

                    /* None is always converted to NULL; this is an
                       optimization over the adapting code and can go away in
                       the future if somebody finds a None adapter usefull. */
                    if (value == Py_None) {
                        t = PyString_FromString("NULL");
                        PyDict_SetItem(n, key, t);
                        /* t is a new object, refcnt = 1, key is at 2 */

                        /* if the value is None we need to substitute the
                           formatting char with 's' (FIXME: this should not be
                           necessary if we drop support for formats other than
                           %s!) */
                        while (*d && !isalpha(*d)) d++;
                        if (*d) *d = 's';
                    }
                    else {
                        t = microprotocol_getquoted(value, conn);

                        if (t != NULL) {
                            PyDict_SetItem(n, key, t);
                            /* both key and t refcnt +1, key is at 2 now */
                        }
                        else {
                            /* no adapter found, raise a BIG exception */
                            Py_XDECREF(value);
                            Py_DECREF(n);
                            return -1;
                        }
                    }

                    Py_XDECREF(t); /* t dies here */
                    /* after the DECREF value has the original refcnt plus 1
                       if it was added to the dictionary directly; good */
                    Py_XDECREF(value);
                }
                else {
                    /* we have an item with one extra refcnt here, zap! */
                    Py_DECREF(item);
                }
                Py_DECREF(key); /* key has the original refcnt now */
                Dprintf("_mogrify: after value refcnt: "
                    FORMAT_CODE_PY_SSIZE_T,
                    value->ob_refcnt
                  );
            }
            c = d;
        }

        else if (c[0] == '%' && c[1] != '(') {
            /* this is a format that expects a tuple; it is much easier,
               because we don't need to check the old/new dictionary for
               keys */

            /* check if some crazy guy mixed formats */
            if (kind == 1) {
                Py_XDECREF(n);
                psyco_set_error(ProgrammingError, (PyObject*)conn,
                  "argument formats can't be mixed", NULL, NULL);
                return -1;
            }
            kind = 2;

            value = PySequence_GetItem(var, index);
            /* value has refcnt inc'ed by 1 here */

            /*  if value is NULL this is not a sequence or the index is wrong;
                anyway we let python set its own exception */
            if (value == NULL) {
                Py_XDECREF(n);
                return -1;
            }

            if (n == NULL) {
                n = PyTuple_New(PyObject_Length(var));
            }

            /* let's have d point just after the '%' */
            d = c+1;

            if (value == Py_None) {
                PyTuple_SET_ITEM(n, index, PyString_FromString("NULL"));
                while (*d && !isalpha(*d)) d++;
                if (*d) *d = 's';
                Py_DECREF(value);
            }
            else {
                PyObject *t = microprotocol_getquoted(value, conn);

                if (t != NULL) {
                    PyTuple_SET_ITEM(n, index, t);
                    Py_DECREF(value);
                }
                else {
                    Py_DECREF(n);
                    Py_DECREF(value);
                    return -1;
                }
            }
            c = d;
            index += 1;
        }
        else {
            c++;
        }
    }

    if (force && n == NULL)
        n = PyTuple_New(0);
    *new = n;

    return 0;
}

void _init_pargs(struct pq_exec_args *pargs){
    memset(pargs, '\0', sizeof(*pargs));
}

void _resize_pargs(struct pq_exec_args *pargs, int nParams){
    int nd;
    if (pargs->nParams && pargs->paramValues){
        if (nParams <= pargs->nParams)
            return;
        nd = nParams - pargs->nParams;
        pargs->paramTypes = (Oid*) PyMem_Realloc(pargs->paramTypes, nParams* sizeof(Oid));
        pargs->paramValues = (char**) PyMem_Realloc(pargs->paramValues, nParams * sizeof(char*));
        pargs->paramLengths = (int*) PyMem_Realloc(pargs->paramLengths, nParams * sizeof(int));
        pargs->paramFormats = (int*) PyMem_Realloc(pargs->paramFormats, nParams * sizeof(int));
        pargs->obRefs = (PyObject**) PyMem_Realloc(pargs->obRefs,nParams * sizeof(PyObject*));
        memset(pargs->paramTypes + pargs->nParams, '\0', nd * sizeof(Oid));
        memset(pargs->paramValues + pargs->nParams, '\0', nd * sizeof(char*));
        memset(pargs->paramLengths + pargs->nParams, '\0', nd * sizeof(int));
        memset(pargs->paramFormats + pargs->nParams, '\0', nd * sizeof(int));
        memset(pargs->obRefs + pargs->nParams, '\0', nd * sizeof(PyObject *));
    }
    else {
        pargs->paramTypes = (Oid*) PyMem_Malloc(nParams* sizeof(Oid));
        pargs->paramValues = (char**) PyMem_Malloc(nParams * sizeof(char*));
        pargs->paramLengths = (int*) PyMem_Malloc(nParams * sizeof(int));
        pargs->paramFormats = (int*) PyMem_Malloc(nParams * sizeof(int));
        pargs->obRefs = (PyObject**) PyMem_Malloc(nParams * sizeof(PyObject*));
        memset(pargs->paramTypes , '\0', nParams * sizeof(Oid));
        memset(pargs->paramValues, '\0', nParams * sizeof(char*));
        memset(pargs->paramLengths, '\0', nParams * sizeof(int));
        memset(pargs->paramFormats, '\0', nParams * sizeof(int));
        memset(pargs->obRefs, '\0', nParams * sizeof(PyObject*));
        Dprintf ("Created %d args at %p", nParams, pargs->paramValues);
        //PyMem_Free(pargs->paramValues);
    }

    pargs->nParams = nParams;
}

void _free_pargs(struct pq_exec_args *pargs){

    int i;
    for(i=0; i < pargs->nParams; i++)
        if (pargs->paramValues[i]) {
            if (pargs->obRefs[i])
                Py_DECREF(pargs->obRefs[i]);
            else
                PyMem_Free(pargs->paramValues[i]);
            pargs->paramValues[i] = NULL;
        }
    
    if (pargs->paramValues ){
        Dprintf("Freeing %d parameters at %p", pargs->nParams, pargs->paramValues);
        PyMem_Free(pargs->paramTypes);
        PyMem_Free(pargs->paramValues);
        PyMem_Free(pargs->paramLengths);
        PyMem_Free(pargs->paramFormats);
        PyMem_Free(pargs->obRefs);
    }
    if (pargs->command){
        PyMem_Free(pargs->command);
        pargs->command = NULL;
    }
    pargs->nParams = 0;
    pargs->paramTypes=NULL;
    pargs->paramValues=NULL;
    pargs->paramLengths=NULL;
    pargs->paramFormats=NULL;
    pargs->obRefs=NULL;
}

/** Return the decimal length of int d
   Note: does NOT work for negative values.*/
/*inline*/ int deci_len(int d){
    int res;
    for (res=1; d >= 10 && res< 20; res++)
        d /= 10;
    return res;
}

/** ensure buf can hold [new] characters.
    Realloc is an expensive operation, so use an aggressive algorithm
    to minimize the need of successive allocations.
    @param buf is the buffer
    @param rptr if not null, rptr is the 'running pointer' inside buf, that
            may need to be adjusted to the new buf
*/
int resize_charbuf(char** buf, char** rptr,
	Py_ssize_t new, Py_ssize_t *cur){
    char *nbuf;
    if (new < *cur)
        return 0;
    else if (*cur == 0){ /* new buffer, only alloc requested size */
        nbuf = (char*) PyMem_Malloc(new);
        if (! nbuf)
            return -1;
        *cur = new;
        /* if (zero)
            memset(nbuf, '\0', new); */
        *buf = nbuf;
        return 0;
    }
    else {
        new += 16;
        nbuf = (char*) PyMem_Realloc(*buf, new);
        if (! nbuf)
            return -1;
        if (rptr && (*buf != nbuf)){
            *rptr = nbuf + (*rptr - *buf);
        }
        *buf = nbuf;
        *cur = new;
        return 0;
    }
    
}

/* mogrify a query string and build argument array or dict */
static int
_mogrify_execparams(PyObject *var, PyObject *fmt, connectionObject *conn, 
        struct pq_exec_args *pargs)
{
    PyObject *key, *value, *item;
    char *d, *c, *c_begin;
    int kind = 0, nParams = 0, index = 0, oindex = 0;
    int i, ri;
    int is_multi = 0;
    
    /* The command buffer. We avoid a python object, because we don't really
    want one. We allocate cmd_alloc bytes, when our estimates need cmdlen.
    The start of the buffer is at cmd_begin, while the pointer runs at cmd.
    */
    char *rs, *rs_begin = NULL;
    Py_ssize_t cmdlen = 0, cmd_alloc = 0;
    
    /* An extra buffer, allocated by the microprotocol_addparams */
    char *nbuf = NULL;
    int nlen = 0;
    
    c = c_begin = PyString_AsString(fmt);
    if (!strncasecmp(c, "comment ", 8))
        return -2; /* that one doesn't like pq_execparams */

    if (!strncasecmp(c, "execute ", 8))
        return -2; /* that one doesn't like pq_execparams FIXME */

    /* First pass: scan the query string for number of arguments, kind
       of format (dict or sequence) and length of pq-formatted query string.
       */
    while(*c) {
        /* handle plain percent symbol in format string */
        if (c[0] == '%' && c[1] == '%') {
            c+=2;
            cmdlen++;
        }
        else if (c[0] == '$' && c[1] == '$') {
            c+=2;
            cmdlen++;
        }
        else if (c[0] == '%' && c[1] == '(') {
        /* if we find '%(' then this is a dictionary, we find the matching ')'
        */
            /* check if some crazy guy mixed formats */
            if (kind && (kind != 1)) {
                psyco_set_error(ProgrammingError, (PyObject*)conn,
                   "argument formats can't be mixed", NULL, NULL);
                return -1;
            }
            kind = 1;

            /* let's have d point the end of the argument */
            for (d = c + 2; *d && *d != ')'; d++);
            
            while (*d && !isalpha(*d)) d++;
            
            nParams++;
            cmdlen+= deci_len(nParams)+1;

            c = d;
        }
        else if (c[0] == '%' && c[1] != '(') {
            /* this is a format that expects a tuple; it is much easier,
               because we don't need to check the old/new dictionary for
               keys */

            /* check if some crazy guy mixed formats */
            if (kind && (kind != 2)) {
                psyco_set_error(ProgrammingError, (PyObject*)conn,
                  "argument formats can't be mixed", NULL, NULL);
                return -1;
            }
            kind = 2;

            nParams++;
            cmdlen+= deci_len(nParams)+1;

            /* let's have d point just after the '%' */
            d = c+1;

            while (*d && !isalpha(*d)) d++;
            c = d;
        }
        else if (c[0] == '$' && c[1] != '$') {
            /* check if some crazy guy mixed formats */
            if (kind > 0) {
                psyco_set_error(ProgrammingError, (PyObject*)conn,
                  "SQL $x parameters are not allowed in parameter queries", NULL, NULL);
                return -1;
            }
            kind = 3;
            cmdlen +=2;
            c+=2;
        }
        else {
            if ( *c == ';'){
                Dprintf("locate semicolon");
                is_multi ++;
            }
            else if (is_multi && ! isspace(*c)){
                Dprintf("This algo cannot handle multiple queries!");
                return -2;
            }
            c++;
            cmdlen++;
        }
    }
    
    c = c_begin;
    _resize_pargs(pargs, nParams);
    if (resize_charbuf(&rs_begin, NULL, cmdlen+1, &cmd_alloc) == -1)
        return -1;
    rs = rs_begin;
    
    /* Second pass: really format the resulting string and the parameter
       lists */
    while(*c) {
        if (c[0] == '%' && c[1] == '%') {
            *(rs++) = '%';
            c+=2;
        }
        else if (c[0] == '%' && c[1] == '(') {

            /* let's have d point the end of the argument */
            for (d = c + 2; *d && *d != ')'; d++);

            if (*d == ')') {
                key = PyString_FromStringAndSize(c+2, (Py_ssize_t) (d-c-2));
                value = PyObject_GetItem(var, key);
                /* key has refcnt 1, value the original value + 1 */

                /*  if value is NULL we did not find the key (or this is not a
                    dictionary): let python raise a KeyError */
                if (value == NULL) {
                    Py_DECREF(key); /* destroy key */
                    PyMem_Free(rs_begin);
                    return -1;
                }
                Py_DECREF(key);
                
                Dprintf("_mogrify_execparams: value refcnt: "
                  FORMAT_CODE_PY_SSIZE_T " (+1)", value->ob_refcnt);

                d++;
                /* This is the place where we *ignore* width specifier
                   and/or sign (like %-10s)
                */
                if (*d && !isalpha(*d)){
                        Dprintf("_mogrify_execparams: width specifier!");
                }
                
                while (*d && !isalpha(*d)) d++;

                if (*d) d++; /* skip the 's' character */

                if ((ri = microprotocol_addparams(value, conn, pargs, oindex, &nbuf, &nlen)) < 0){
                        Py_XDECREF(value);
                        PyMem_Free(rs_begin);
                        return ri;
                }else{
                    if ((d-c) > 2)
                        cmdlen -= (d-c) - 1;
                    if (nlen) {
                        cmdlen += nlen;
                    }
                    else if (ri > 0) {
                        cmdlen += deci_len(oindex + ri)+1;
                    }
                    
                    if (resize_charbuf(&rs_begin, &rs, cmdlen, &cmd_alloc) == -1){
                        psyco_set_error(InternalError, NULL, NULL, NULL, NULL);
                        Py_XDECREF(value);
                        PyMem_Free(rs_begin);
                        return -1;
                    }
                    
                    if (nlen){
                        for (i = 0; i < nlen ; i++)
                            *(rs++) = nbuf[i];
                    }else if (ri == 1) {
                        i = sprintf(rs,"$%d", oindex+1);
                        rs += i;
                    }
                    oindex += ri;
                    Py_DECREF(value);
                }
                
                Dprintf("_mogrify_execparams: after value refcnt: "
                    FORMAT_CODE_PY_SSIZE_T,
                    value->ob_refcnt
                  );
            }
            c = d;
        }
        else if (c[0] == '%' && c[1] != '(') {
            value = PySequence_GetItem(var, index++);
            /* value has refcnt inc'ed by 1 here */

            /*  if value is NULL this is not a sequence or the index is wrong;
                anyway we let python set its own exception */
            if (value == NULL) {
                Dprintf("Couldn't get %d th value of an %s", index-1, var->ob_type->tp_name);
                /* Py_XDECREF(n); */
                return -1;
            }
            Dprintf("Got args[%d] = %s", index-1, value->ob_type->tp_name);
            Dprintf("_mogrify_execparams: before value refcnt: "
                FORMAT_CODE_PY_SSIZE_T,
                value->ob_refcnt
                );
            
            d = c+1;
            if (*d && !isalpha(*d)){
                Dprintf("_mogrify_execparams: width specifier!");
            }
                
            while (*d && !isalpha(*d)) d++;

            if (*d) d++; /* skip the 's' character */

            if ((ri = microprotocol_addparams(value, conn, pargs, oindex, &nbuf, &nlen)) < 0){
                Py_XDECREF(value);
                return ri;
            }else{
                if ((d-c) > 2)
                    cmdlen -= (d-c) - 1;
                if (nlen) {
                    cmdlen += nlen;
                }
                else if (ri > 0) {
                    cmdlen += deci_len(oindex + 1)+1;
                }
                
                if (resize_charbuf(&rs_begin, &rs, cmdlen, &cmd_alloc) == -1){
                    psyco_set_error(InternalError, NULL, NULL, NULL, NULL);
                    Py_XDECREF(value);
                    PyMem_Free(rs_begin);
                    return -1;
                }
                
                if (nlen){
                    for (i = 0; i < nlen ; i++)
                        *(rs++) = nbuf[i];
                }else if (ri == 1) {
                    i = sprintf(rs,"$%d",oindex+1);
                    rs += i;
                }
                oindex += ri;
                Py_DECREF(value);
            }
            
            Dprintf("_mogrify_execparams: after value refcnt: "
                FORMAT_CODE_PY_SSIZE_T,
                value->ob_refcnt
                );

            c = d;
        }
        else {
            *(rs++) = *(c++);
        }
    }
    
    *rs = '\0';
    Dprintf ("_mogrify_execparams: result string %ld/%ld : %.100s", 
             (rs - rs_begin), cmdlen, rs_begin);
    pargs->command = rs_begin;

    return 0;
}

static PyObject *_psyco_curs_validate_sql_basic(
    cursorObject *self, PyObject *sql
  )
{
    /* Performs very basic validation on an incoming SQL string.
       Returns a new reference to a str instance on success; NULL on failure,
       after having set an exception. */

    if (!sql || !PyObject_IsTrue(sql)) {
        psyco_set_error(ProgrammingError, (PyObject*)self,
                         "can't execute an empty query", NULL, NULL);
        goto fail;
    }

    if (PyString_Check(sql)) {
        /* Necessary for ref-count symmetry with the unicode case: */
        Py_INCREF(sql);
    }
    else if (PyUnicode_Check(sql)) {
        PyObject *enc = PyDict_GetItemString(psycoEncodings,
                                             self->conn->encoding);
        /* enc is a borrowed reference; we won't decref it */

        if (enc) {
            sql = PyUnicode_AsEncodedString(sql, PyString_AsString(enc), NULL);
            /* if there was an error during the encoding from unicode to the
               target encoding, we just let the exception propagate */
            if (sql == NULL) { goto fail; }
        } else {
            PyErr_Format(InterfaceError,
                         "can't encode unicode SQL statement to %s",
                         self->conn->encoding);
            goto fail;
        }
    }
    else {
        /* the  is not unicode or string, raise an error */
        PyErr_SetString(PyExc_TypeError,
                        "argument 1 must be a string or unicode object");
        goto fail;
    }

    return sql; /* new reference */
    fail:
        return NULL;
}

/* Merge together a query string and its arguments.
 *
 * The arguments have been already adapted to SQL.
 *
 * Return a new reference to a string with the merged query,
 * NULL and set an exception if any happened.
 */
static PyObject *
_psyco_curs_merge_query_args(cursorObject *self,
                             PyObject *query, PyObject *args)
{
    PyObject *fquery;

    /* if PyString_Format() return NULL an error occured: if the error is
       a TypeError we need to check the exception.args[0] string for the
       values:

           "not enough arguments for format string"
           "not all arguments converted"

       and return the appropriate ProgrammingError. we do that by grabbing
       the curren exception (we will later restore it if the type or the
       strings do not match.) */

    if (!(fquery = PyString_Format(query, args))) {
        PyObject *err, *arg, *trace;
        int pe = 0;

        PyErr_Fetch(&err, &arg, &trace);

        if (err && PyErr_GivenExceptionMatches(err, PyExc_TypeError)) {
            Dprintf("psyco_curs_execute: TypeError exception catched");
            PyErr_NormalizeException(&err, &arg, &trace);

            if (PyObject_HasAttrString(arg, "args")) {
                PyObject *args = PyObject_GetAttrString(arg, "args");
                PyObject *str = PySequence_GetItem(args, 0);
                const char *s = PyString_AS_STRING(str);

                Dprintf("psyco_curs_execute:     -> %s", s);

                if (!strcmp(s, "not enough arguments for format string")
                  || !strcmp(s, "not all arguments converted")) {
                    Dprintf("psyco_curs_execute:     -> got a match");
                    psyco_set_error(ProgrammingError, (PyObject*)self,
                                     s, NULL, NULL);
                    pe = 1;
                }

                Py_DECREF(args);
                Py_DECREF(str);
            }
        }

        /* if we did not manage our own exception, restore old one */
        if (pe == 1) {
            Py_XDECREF(err); Py_XDECREF(arg); Py_XDECREF(trace);
        }
        else {
            PyErr_Restore(err, arg, trace);
        }
    }

    return fquery;
}

#define psyco_curs_execute_doc \
"execute(query, vars=None) -- Execute query with bound vars."

static int
_psyco_curs_execute(cursorObject *self,
                    PyObject *operation, PyObject *vars, long int async)
{
    int res = 0, mres = 0;
    PyObject *fquery, *cvt = NULL;
    struct pq_exec_args pargs;
    _init_pargs(&pargs);
    pargs.nParams = 0;

    operation = _psyco_curs_validate_sql_basic(self, operation);

    /* Any failure from here forward should 'goto fail' rather than 'return 0'
       directly. */

    if (operation == NULL) { goto fail; }

    IFCLEARPGRES(self->pgres);

    if (self->query) {
        Py_DECREF(self->query);
        self->query = NULL;
    }

    Dprintf("psyco_curs_execute: starting execution of new query");

    /* here we are, and we have a sequence or a dictionary filled with
       objects to be substituted (bound variables). we try to be smart and do
       the right thing (i.e., what the user expects) */

    if (vars && vars != Py_None)
    {
#ifdef HAVE_EXECPARAMS
        if ((mres = _mogrify_execparams(vars, operation, self->conn, &pargs)) == -1) 
            goto fail;
        else if (mres == -2){ /* retry the old way */
            Dprintf("Fallback to the old pq_execute code");
#ifdef PSYCOPG_DEBUG
            if (PyErr_Occurred())
                PyErr_Print();
#endif
            PyErr_Clear();
#else /* no EXECPARAMS */
        if (1) {
#endif
            if(_mogrify(vars, operation, self->conn, &cvt) == -1) {
                goto fail;
            }
        }
        
    }

    if (vars && cvt) {
        /* Doesn't happen if exec params have been prepared */
        if (!(fquery = _psyco_curs_merge_query_args(self, operation, cvt))) {
            goto fail;
        }

        if (self->name != NULL) {
            self->query = PyString_FromFormat(
                "DECLARE %s CURSOR WITHOUT HOLD FOR %s",
                self->name, PyString_AS_STRING(fquery));
            Py_DECREF(fquery);
        }
        else {
            self->query = fquery;
        }
    }
    else {
        if (self->name != NULL) {
            self->query = PyString_FromFormat(
                "DECLARE %s CURSOR WITHOUT HOLD FOR %s",
                self->name, PyString_AS_STRING(operation));
        }
        else {
            /* Transfer reference ownership of the str in operation to
               self->query, clearing the local variable to prevent cleanup from
               DECREFing it */
            self->query = operation;
            operation = NULL;
        }
    }

    /* At this point, the SQL statement must be str, not unicode */

    if (pargs.nParams && mres >= 0)
        res = pq_execute_params(self, &pargs, async);
    else
        res = pq_execute(self, PyString_AS_STRING(self->query), async);
    Dprintf("psyco_curs_execute: res = %d, pgres = %p", res, self->pgres);
    if (res == -1) { goto fail; }

    res = 1; /* Success */
    goto cleanup;

    fail:
        res = 0;
        /* Fall through to cleanup */
    cleanup:
        /* Py_XDECREF(operation) is safe because the original reference passed
           by the caller was overwritten with either NULL or a new
           reference */
        Py_XDECREF(operation);

        Py_XDECREF(cvt);
        _free_pargs(&pargs);

        return res;
}

static PyObject *
psyco_curs_execute(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *vars = NULL, *operation = NULL;

    static char *kwlist[] = {"query", "vars", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O", kwlist,
                                     &operation, &vars)) {
        return NULL;
    }

    if (self->name != NULL) {
        if (self->query != Py_None) {
            psyco_set_error(ProgrammingError, (PyObject*)self,
                "can't call .execute() on named cursors more than once",
                NULL, NULL);
            return NULL;
        }
        if (self->conn->isolation_level == ISOLATION_LEVEL_AUTOCOMMIT) {
            psyco_set_error(ProgrammingError, (PyObject*)self,
                "can't use a named cursor outside of transactions", NULL, NULL);
            return NULL;
        }
        if (self->conn->mark != self->mark) {
            psyco_set_error(ProgrammingError, (PyObject*)self,
                "named cursor isn't valid anymore", NULL, NULL);
            return NULL;
        }
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, execute);
    EXC_IF_TPC_PREPARED(self->conn, execute);

    if (_psyco_curs_execute(self, operation, vars, self->conn->async)) {
        Py_INCREF(Py_None);
        return Py_None;
    }
    else {
        return NULL;
    }
}

#define psyco_curs_executemany_doc \
"executemany(query, vars_list) -- Execute many queries with bound vars."

static PyObject *
psyco_curs_executemany(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *operation = NULL, *vars = NULL;
    PyObject *v, *iter = NULL;
    int rowcount = 0;

    static char *kwlist[] = {"query", "vars_list", NULL};

    /* reset rowcount to -1 to avoid setting it when an exception is raised */
    self->rowcount = -1;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", kwlist,
                                     &operation, &vars)) {
        return NULL;
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, executemany);
    EXC_IF_TPC_PREPARED(self->conn, executemany);

    if (self->name != NULL) {
        psyco_set_error(ProgrammingError, (PyObject*)self,
                "can't call .executemany() on named cursors", NULL, NULL);
        return NULL;
    }

    if (!PyIter_Check(vars)) {
        vars = iter = PyObject_GetIter(vars);
        if (iter == NULL) return NULL;
    }

    while ((v = PyIter_Next(vars)) != NULL) {
        if (_psyco_curs_execute(self, operation, v, 0) == 0) {
            Py_DECREF(v);
            Py_XDECREF(iter);
            return NULL;
        }
        else {
            if (self->rowcount == -1)
                rowcount = -1;
            else if (rowcount >= 0)
                rowcount += self->rowcount;
            Py_DECREF(v);
        }
    }
    Py_XDECREF(iter);
    self->rowcount = rowcount;

    if (!PyErr_Occurred()) {
        Py_INCREF(Py_None);
        return Py_None;
    }
    else {
        return NULL;
    }
}


#ifdef PSYCOPG_EXTENSIONS
#define psyco_curs_mogrify_doc \
"mogrify(query, vars=None) -> str -- Return query after vars binding."

static PyObject *
_psyco_curs_mogrify(cursorObject *self,
                   PyObject *operation, PyObject *vars)
{
    PyObject *fquery = NULL, *cvt = NULL;

    operation = _psyco_curs_validate_sql_basic(self, operation);
    if (operation == NULL) { goto cleanup; }

    Dprintf("psyco_curs_mogrify: starting mogrify");

    /* here we are, and we have a sequence or a dictionary filled with
       objects to be substituted (bound variables). we try to be smart and do
       the right thing (i.e., what the user expects) */

    if (vars && vars != Py_None)
    {
        if (_mogrify(vars, operation, self->conn, &cvt) == -1) {
            goto cleanup;
        }
    }

    if (vars && cvt) {
        if (!(fquery = _psyco_curs_merge_query_args(self, operation, cvt))) {
            goto cleanup;
        }

        Dprintf("psyco_curs_mogrify: cvt->refcnt = " FORMAT_CODE_PY_SSIZE_T
            ", fquery->refcnt = " FORMAT_CODE_PY_SSIZE_T,
            cvt->ob_refcnt, fquery->ob_refcnt);
    }
    else {
        fquery = operation;
        Py_INCREF(fquery);
    }

cleanup:
    Py_XDECREF(operation);
    Py_XDECREF(cvt);

    return fquery;
}

static PyObject *
psyco_curs_mogrify(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *vars = NULL, *operation = NULL;

    static char *kwlist[] = {"query", "vars", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O", kwlist,
                                     &operation, &vars)) {
        return NULL;
    }

    EXC_IF_CURS_CLOSED(self);

    return _psyco_curs_mogrify(self, operation, vars);
}
#endif


/* fetchone method - fetch one row of results */

#define psyco_curs_fetchone_doc \
"fetchone() -> tuple or None\n\n" \
"Return the next row of a query result set in the form of a tuple (by\n" \
"default) or using the sequence factory previously set in the\n" \
"`row_factory` attribute. Return `None` when no more data is available.\n"

static int
_psyco_curs_prefetch(cursorObject *self)
{
    int i = 0;

    if (self->pgres == NULL) {
        Dprintf("_psyco_curs_prefetch: trying to fetch data");
        do {
            i = pq_fetch(self);
            Dprintf("_psycopg_curs_prefetch: result = %d", i);
        } while(i == 1);
    }

    Dprintf("_psyco_curs_prefetch: result = %d", i);
    return i;
}

static PyObject *
_psyco_curs_buildrow_fill(cursorObject *self, PyObject *res,
                          int row, int n, int istuple)
{
    int i, len, err;
    const char *str;
    PyObject *val;

    for (i=0; i < n; i++) {
        if (PQgetisnull(self->pgres, row, i)) {
            str = NULL;
            len = 0;
        }
        else {
            str = PQgetvalue(self->pgres, row, i);
            len = PQgetlength(self->pgres, row, i);
        }

        Dprintf("_psyco_curs_buildrow: row %ld, element %d, len %d",
                self->row, i, len);

        val = typecast_cast(PyTuple_GET_ITEM(self->casts, i), str, len,
                            (PyObject*)self);

        if (val) {
            Dprintf("_psyco_curs_buildrow: val->refcnt = "
                FORMAT_CODE_PY_SSIZE_T,
                val->ob_refcnt
              );
            if (istuple) {
                PyTuple_SET_ITEM(res, i, val);
            }
            else {
                err = PySequence_SetItem(res, i, val);
                Py_DECREF(val);
                if (err == -1) {
                    Py_DECREF(res);
                    res = NULL;
                    break;
                }
            }
        }
        else {
            /* an error occurred in the type system, we return NULL to raise
               an exception. the typecast code should already have set the
               exception type and text */
            Py_DECREF(res);
            res = NULL;
            break;
        }
    }
    return res;
}

static PyObject *
_psyco_curs_buildrow(cursorObject *self, int row)
{
    int n;

    n = PQnfields(self->pgres);
    return _psyco_curs_buildrow_fill(self, PyTuple_New(n), row, n, 1);
}

static PyObject *
_psyco_curs_buildrow_with_factory(cursorObject *self, int row)
{
    int n;
    PyObject *res;

    n = PQnfields(self->pgres);
    if (!(res = PyObject_CallFunctionObjArgs(self->tuple_factory, self, NULL)))
        return NULL;

    return _psyco_curs_buildrow_fill(self, res, row, n, 0);
}

static PyObject *
psyco_curs_fetchone(cursorObject *self, PyObject *args)
{
    PyObject *res;

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, fetchone);
    if (_psyco_curs_prefetch(self) < 0) return NULL;
    EXC_IF_NO_TUPLES(self);

    if (self->name != NULL) {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_TPC_PREPARED(self->conn, fetchone);
        PyOS_snprintf(buffer, 127, "FETCH FORWARD 1 FROM %s", self->name);
        if (pq_execute(self, buffer, 0) == -1) return NULL;
        if (_psyco_curs_prefetch(self) < 0) return NULL;
    }

    Dprintf("psyco_curs_fetchone: fetching row %ld", self->row);
    Dprintf("psyco_curs_fetchone: rowcount = %ld", self->rowcount);

    if (self->row >= self->rowcount) {
        /* we exausted available data: return None */
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (self->tuple_factory == Py_None)
        res = _psyco_curs_buildrow(self, self->row);
    else
        res = _psyco_curs_buildrow_with_factory(self, self->row);

    self->row++; /* move the counter to next line */

    /* if the query was async aggresively free pgres, to allow
       successive requests to reallocate it */
    if (self->row >= self->rowcount
        && self->conn->async_cursor == (PyObject*)self)
        IFCLEARPGRES(self->pgres);

    return res;
}


/* fetch many - fetch some results */

#define psyco_curs_fetchmany_doc \
"fetchmany(size=self.arraysize) -> list of tuple\n\n" \
"Return the next `size` rows of a query result set in the form of a list\n" \
"of tuples (by default) or using the sequence factory previously set in\n" \
"the `row_factory` attribute. Return `None` when no more data is available.\n"

static PyObject *
psyco_curs_fetchmany(cursorObject *self, PyObject *args, PyObject *kwords)
{
    int i;
    PyObject *list, *res;

    long int size = self->arraysize;
    static char *kwlist[] = {"size", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwords, "|l", kwlist, &size)) {
        return NULL;
    }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, fetchmany);
    if (_psyco_curs_prefetch(self) < 0) return NULL;
    EXC_IF_NO_TUPLES(self);

    if (self->name != NULL) {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_TPC_PREPARED(self->conn, fetchone);
        PyOS_snprintf(buffer, 127, "FETCH FORWARD %d FROM %s",
            (int)size, self->name);
        if (pq_execute(self, buffer, 0) == -1) return NULL;
        if (_psyco_curs_prefetch(self) < 0) return NULL;
    }

    /* make sure size is not > than the available number of rows */
    if (size > self->rowcount - self->row || size < 0) {
        size = self->rowcount - self->row;
    }

    Dprintf("psyco_curs_fetchmany: size = %ld", size);

    if (size <= 0) {
        return PyList_New(0);
    }

    list = PyList_New(size);

    for (i = 0; i < size; i++) {
        if (self->tuple_factory == Py_None)
            res = _psyco_curs_buildrow(self, self->row);
        else
            res = _psyco_curs_buildrow_with_factory(self, self->row);

        self->row++;

        if (res == NULL) {
            Py_DECREF(list);
            return NULL;
        }

        PyList_SET_ITEM(list, i, res);
    }

    /* if the query was async aggresively free pgres, to allow
       successive requests to reallocate it */
    if (self->row >= self->rowcount
        && self->conn->async_cursor == (PyObject*)self)
        IFCLEARPGRES(self->pgres);

    return list;
}


/* fetch all - fetch all results */

#define psyco_curs_fetchall_doc \
"fetchall() -> list of tuple\n\n" \
"Return all the remaining rows of a query result set.\n\n" \
"Rows are returned in the form of a list of tuples (by default) or using\n" \
"the sequence factory previously set in the `row_factory` attribute.\n" \
"Return `None` when no more data is available.\n"

static PyObject *
psyco_curs_fetchall(cursorObject *self, PyObject *args)
{
    int i, size;
    PyObject *list, *res;

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, fetchall);
    if (_psyco_curs_prefetch(self) < 0) return NULL;
    EXC_IF_NO_TUPLES(self);

    if (self->name != NULL) {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_TPC_PREPARED(self->conn, fetchall);
        PyOS_snprintf(buffer, 127, "FETCH FORWARD ALL FROM %s", self->name);
        if (pq_execute(self, buffer, 0) == -1) return NULL;
        if (_psyco_curs_prefetch(self) < 0) return NULL;
    }

    size = self->rowcount - self->row;

    if (size <= 0) {
        return PyList_New(0);
    }

    list = PyList_New(size);

    for (i = 0; i < size; i++) {
        if (self->tuple_factory == Py_None)
            res = _psyco_curs_buildrow(self, self->row);
        else
            res = _psyco_curs_buildrow_with_factory(self, self->row);

        self->row++;

        if (res == NULL) {
            Py_DECREF(list);
            return NULL;
        }

        PyList_SET_ITEM(list, i, res);
    }

    /* if the query was async aggresively free pgres, to allow
       successive requests to reallocate it */
    if (self->row >= self->rowcount
        && self->conn->async_cursor == (PyObject*)self)
        IFCLEARPGRES(self->pgres);

    return list;
}


/* callproc method - execute a stored procedure */

#define psyco_curs_callproc_doc \
"callproc(procname, parameters=None) -- Execute stored procedure."

static PyObject *
psyco_curs_callproc(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    const char *procname = NULL;
    char *sql = NULL;
    Py_ssize_t procname_len, i, nparameters = 0, sl = 0;
    PyObject *parameters = Py_None;
    PyObject *operation = NULL;
    PyObject *res = NULL;

    if (!PyArg_ParseTuple(args, "s#|O",
          &procname, &procname_len, &parameters
       ))
    { return NULL; }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, callproc);
    EXC_IF_TPC_PREPARED(self->conn, callproc);

    if (self->name != NULL) {
        psyco_set_error(ProgrammingError, (PyObject*)self,
                         "can't call .callproc() on named cursors", NULL, NULL);
        return NULL;
    }

    if(parameters != Py_None) {
        nparameters = PyObject_Length(parameters);
        if (nparameters < 0) nparameters = 0;
    }

    /* allocate some memory, build the SQL and create a PyString from it */
    sl = procname_len + 17 + nparameters*3 - (nparameters ? 1 : 0);
    sql = (char*)PyMem_Malloc(sl);
    if (sql == NULL) return NULL;

    sprintf(sql, "SELECT * FROM %s(", procname);
    for(i=0; i<nparameters; i++) {
        strcat(sql, "%s,");
    }
    sql[sl-2] = ')';
    sql[sl-1] = '\0';

    operation = PyString_FromString(sql);
    PyMem_Free((void*)sql);

    if (_psyco_curs_execute(self, operation, parameters, self->conn->async)) {
        Py_INCREF(parameters);
        res = parameters;
    }

    Py_DECREF(operation);
    return res;
}


/* nextset method - return the next set of data (not supported) */

#define psyco_curs_nextset_doc \
"nextset() -- Skip to next set of data.\n\n" \
"This method is not supported (PostgreSQL does not have multiple data \n" \
"sets) and will raise a NotSupportedError exception."

static PyObject *
psyco_curs_nextset(cursorObject *self, PyObject *args)
{
    EXC_IF_CURS_CLOSED(self);

    PyErr_SetString(NotSupportedError, "not supported by PostgreSQL");
    return NULL;
}


/* setinputsizes - predefine memory areas for execute (does nothing) */

#define psyco_curs_setinputsizes_doc \
"setinputsizes(sizes) -- Set memory areas before execute.\n\n" \
"This method currently does nothing but it is safe to call it."

static PyObject *
psyco_curs_setinputsizes(cursorObject *self, PyObject *args)
{
    PyObject *sizes;

    if (!PyArg_ParseTuple(args, "O", &sizes))
        return NULL;

    EXC_IF_CURS_CLOSED(self);

    Py_INCREF(Py_None);
    return Py_None;
}


/* setoutputsize - predefine memory areas for execute (does nothing) */

#define psyco_curs_setoutputsize_doc \
"setoutputsize(size, column=None) -- Set column buffer size.\n\n" \
"This method currently does nothing but it is safe to call it."

static PyObject *
psyco_curs_setoutputsize(cursorObject *self, PyObject *args)
{
    long int size, column;

    if (!PyArg_ParseTuple(args, "l|l", &size, &column))
        return NULL;

    EXC_IF_CURS_CLOSED(self);

    Py_INCREF(Py_None);
    return Py_None;
}


/* scroll - scroll position in result list */

#define psyco_curs_scroll_doc \
"scroll(value, mode='relative') -- Scroll to new position according to mode."

static PyObject *
psyco_curs_scroll(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    int value, newpos;
    const char *mode = "relative";

    static char *kwlist[] = {"value", "mode", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|s",
                                     kwlist, &value, &mode))
        return NULL;

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_ASYNC_IN_PROGRESS(self, scroll)

    /* if the cursor is not named we have the full result set and we can do
       our own calculations to scroll; else we just delegate the scrolling
       to the MOVE SQL statement */
    if (self->name == NULL) {
        if (strcmp(mode, "relative") == 0) {
            newpos = self->row + value;
        } else if (strcmp( mode, "absolute") == 0) {
            newpos = value;
        } else {
            psyco_set_error(ProgrammingError, (PyObject*)self,
                "scroll mode must be 'relative' or 'absolute'", NULL, NULL);
            return NULL;
        }

        if (newpos < 0 || newpos >= self->rowcount ) {
            psyco_set_error(ProgrammingError, (PyObject*)self,
                             "scroll destination out of bounds", NULL, NULL);
            return NULL;
        }

        self->row = newpos;
    }

    else {
        char buffer[128];

        EXC_IF_NO_MARK(self);
        EXC_IF_TPC_PREPARED(self->conn, scroll);

        if (strcmp(mode, "absolute") == 0) {
            PyOS_snprintf(buffer, 127, "MOVE ABSOLUTE %d FROM %s",
                value, self->name);
        }
        else {
            PyOS_snprintf(buffer, 127, "MOVE %d FROM %s", value, self->name);
        }
        if (pq_execute(self, buffer, 0) == -1) return NULL;
        if (_psyco_curs_prefetch(self) < 0) return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}


#ifdef PSYCOPG_EXTENSIONS

static int _psyco_curs_copy_columns(PyObject *columns, char *columnlist)
{
    PyObject *col, *coliter;
    Py_ssize_t collen;
    char* colname;
    Py_ssize_t offset = 1;

    columnlist[0] = '\0';
    if (columns == NULL || columns == Py_None) return 0;

    coliter = PyObject_GetIter(columns);
    if (coliter == NULL) return 0;

    columnlist[0] = '(';

    while ((col = PyIter_Next(coliter)) != NULL) {
        if (!PyString_Check(col)) {
            Py_DECREF(col);
            Py_DECREF(coliter);
            PyErr_SetString(PyExc_ValueError,
                "elements in column list must be strings");
            return -1;
        }
        PyString_AsStringAndSize(col, &colname, &collen);
        if (offset + collen > DEFAULT_COPYBUFF - 2) {
            Py_DECREF(col);
            Py_DECREF(coliter);
            PyErr_SetString(PyExc_ValueError, "column list too long");
            return -1;
        }
        strncpy(&columnlist[offset], colname, collen);
        offset += collen;
        columnlist[offset++] = ',';
        Py_DECREF(col);
    }
    Py_DECREF(coliter);

    /* Error raised by the coliter generator */
    if (PyErr_Occurred()) {
        return -1;
    }

    if (offset == 2) {
        return 0;
    }
    else {
        columnlist[offset - 1] = ')';
        columnlist[offset] = '\0';
        return 1;
    }
}

/* extension: copy_from - implements COPY FROM */

#define psyco_curs_copy_from_doc \
"copy_from(file, table, sep='\\t', null='\\N', columns=None) -- Copy table from file."

static int
_psyco_curs_has_read_check(PyObject* o, void* var)
{
    if (PyObject_HasAttrString(o, "readline")
        && PyObject_HasAttrString(o, "read")) {
        /* It's OK to store a borrowed reference, because it is only held for
         * the duration of psyco_curs_copy_from. */
        *((PyObject**)var) = o;
        return 1;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
            "argument 1 must have both .read() and .readline() methods");
        return 0;
    }
}

static PyObject *
psyco_curs_copy_from(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    char query_buffer[DEFAULT_COPYBUFF];
    Py_ssize_t query_size;
    char *query;
    const char *table_name;
    const char *sep = "\t", *null = NULL;
    Py_ssize_t bufsize = DEFAULT_COPYBUFF;
    PyObject *file, *columns = NULL, *res = NULL;
    char columnlist[DEFAULT_COPYBUFF];
    char *quoted_delimiter;

    static char *kwlist[] = {
            "file", "table", "sep", "null", "size", "columns", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
        "O&s|ss" CONV_CODE_PY_SSIZE_T "O", kwlist,
        _psyco_curs_has_read_check, &file, &table_name, &sep, &null, &bufsize,
        &columns))
    {
        return NULL;
    }

    if (_psyco_curs_copy_columns(columns, columnlist) == -1)
        return NULL;

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, copy_from);
    EXC_IF_GREEN(copy_from);
    EXC_IF_TPC_PREPARED(self->conn, copy_from);


    quoted_delimiter = psycopg_escape_string((PyObject*)self->conn, sep, 0, NULL, NULL);
    if (quoted_delimiter == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    
    query = query_buffer;
    if (null) {
        char *quoted_null = psycopg_escape_string((PyObject*)self->conn, null, 0, NULL, NULL);
        if (quoted_null == NULL) {
            PyMem_Free(quoted_delimiter);
            PyErr_NoMemory();
            return NULL;
        }
        query_size = PyOS_snprintf(query, DEFAULT_COPYBUFF,
            "COPY %s%s FROM stdin WITH DELIMITER AS %s NULL AS %s",
            table_name, columnlist, quoted_delimiter, quoted_null);
        if (query_size >= DEFAULT_COPYBUFF) {
            /* Got truncated, allocate dynamically */
            query = (char *)PyMem_Malloc((query_size + 1) * sizeof(char));
            PyOS_snprintf(query, query_size + 1,
                "COPY %s%s FROM stdin WITH DELIMITER AS %s NULL AS %s",
                table_name, columnlist, quoted_delimiter, quoted_null);
        }
        PyMem_Free(quoted_null);
    }
    else {
        query_size = PyOS_snprintf(query, DEFAULT_COPYBUFF,
           "COPY %s%s FROM stdin WITH DELIMITER AS %s",
           table_name, columnlist, quoted_delimiter);
        if (query_size >= DEFAULT_COPYBUFF) {
            /* Got truncated, allocate dynamically */
            query = (char *)PyMem_Malloc((query_size + 1) * sizeof(char));
            PyOS_snprintf(query, query_size + 1,
                "COPY %s%s FROM stdin WITH DELIMITER AS %s",
                table_name, columnlist, quoted_delimiter);
        }
    }    
    PyMem_Free(quoted_delimiter);
    
    Dprintf("psyco_curs_copy_from: query = %s", query);

    self->copysize = bufsize;
    self->copyfile = file;

    if (pq_execute(self, query, 0) == 1) {
        res = Py_None;
        Py_INCREF(Py_None);
    }

    if (query && (query != query_buffer)) {
        PyMem_Free(query);
    }
    self->copyfile = NULL;

    return res;
}

/* extension: copy_to - implements COPY TO */

#define psyco_curs_copy_to_doc \
"copy_to(file, table, sep='\\t', null='\\N', columns=None) -- Copy table to file."

static int
_psyco_curs_has_write_check(PyObject* o, void* var)
{
    if (PyObject_HasAttrString(o, "write")) {
        /* It's OK to store a borrowed reference, because it is only held for
         * the duration of psyco_curs_copy_to. */
        *((PyObject**)var) = o;
        return 1;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "argument 1 must have a .write() method");
        return 0;
    }
}

static PyObject *
psyco_curs_copy_to(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    char *query = NULL;
    char query_buffer[DEFAULT_COPYBUFF];
    size_t query_size;
    char columnlist[DEFAULT_COPYBUFF];
    const char *table_name;
    const char *sep = "\t", *null = NULL;
    PyObject *file, *columns = NULL, *res = NULL;
    char *quoted_delimiter;

    static char *kwlist[] = {"file", "table", "sep", "null", "columns", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&s|ssO", kwlist,
                                     _psyco_curs_has_write_check, &file,
                                     &table_name, &sep, &null, &columns)) {
        return NULL;
    }

    if (_psyco_curs_copy_columns(columns, columnlist) == -1)
        return NULL;

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, copy_to);
    EXC_IF_GREEN(copy_to);
    EXC_IF_TPC_PREPARED(self->conn, copy_to);

    quoted_delimiter = psycopg_escape_string((PyObject*)self->conn, sep, 0, NULL, NULL);
    if (quoted_delimiter == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    
    query = query_buffer;
    if (null) {
        char *quoted_null = psycopg_escape_string((PyObject*)self->conn, null, 0, NULL, NULL);
        if (NULL == quoted_null) {
            PyMem_Free(quoted_delimiter);
            PyErr_NoMemory();
            return NULL;
        }
        query_size = PyOS_snprintf(query, DEFAULT_COPYBUFF,
            "COPY %s%s TO stdout WITH DELIMITER AS %s"
            " NULL AS %s", table_name, columnlist, quoted_delimiter, quoted_null);
        if (query_size >= DEFAULT_COPYBUFF) {
            /* Got truncated, allocate dynamically */
            query = (char *)PyMem_Malloc((query_size + 1) * sizeof(char));
            PyOS_snprintf(query, query_size + 1,
                "COPY %s%s TO stdout WITH DELIMITER AS %s"
                " NULL AS %s", table_name, columnlist, quoted_delimiter, quoted_null);
        }
        PyMem_Free(quoted_null);
    }
    else {
        query_size = PyOS_snprintf(query, DEFAULT_COPYBUFF,
            "COPY %s%s TO stdout WITH DELIMITER AS %s",
            table_name, columnlist, quoted_delimiter);
        if (query_size >= DEFAULT_COPYBUFF) {
            /* Got truncated, allocate dynamically */
            query = (char *)PyMem_Malloc((query_size + 1) * sizeof(char));
            PyOS_snprintf(query, query_size + 1,
                "COPY %s%s TO stdout WITH DELIMITER AS %s",
                table_name, columnlist, quoted_delimiter);
        }
    }
    PyMem_Free(quoted_delimiter);
    
    Dprintf("psyco_curs_copy_to: query = %s", query);

    self->copysize = 0;
    self->copyfile = file;

    if (pq_execute(self, query, 0) == 1) {
        res = Py_None;
        Py_INCREF(Py_None);
    }
    if (query && (query != query_buffer)) {
        PyMem_Free(query);
    }
    self->copyfile = NULL;

    return res;
}

/* extension: copy_expert - implements extended COPY FROM/TO

   This method supports both COPY FROM and COPY TO with user-specifiable
   SQL statement, rather than composing the statement from parameters.
*/

#define psyco_curs_copy_expert_doc \
"copy_expert(sql, file, size=None) -- Submit a user-composed COPY statement.\n" \
"`file` must be an open, readable file for COPY FROM or an open, writeable\n"   \
"file for COPY TO. The optional `size` argument, when specified for a COPY\n"   \
"FROM statement, will be passed to file's read method to control the read\n"    \
"buffer size."

static PyObject *
psyco_curs_copy_expert(cursorObject *self, PyObject *args, PyObject *kwargs)
{
    Py_ssize_t bufsize = DEFAULT_COPYBUFF;
    PyObject *sql, *file, *res = NULL;

    static char *kwlist[] = {"sql", "file", "size", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
        "OO|" CONV_CODE_PY_SSIZE_T, kwlist, &sql, &file, &bufsize))
    { return NULL; }

    EXC_IF_CURS_CLOSED(self);
    EXC_IF_CURS_ASYNC(self, copy_expert);
    EXC_IF_GREEN(copy_expert);
    EXC_IF_TPC_PREPARED(self->conn, copy_expert);

    sql = _psyco_curs_validate_sql_basic(self, sql);
    
    /* Any failure from here forward should 'goto fail' rather than
       'return NULL' directly. */
    
    if (sql == NULL) { goto fail; }

    /* This validation of file is rather weak, in that it doesn't enforce the
       assocation between "COPY FROM" -> "read" and "COPY TO" -> "write".
       However, the error handling in _pq_copy_[in|out] must be able to handle
       the case where the attempt to call file.read|write fails, so no harm
       done. */
    
    if (   !PyObject_HasAttrString(file, "read")
        && !PyObject_HasAttrString(file, "write")
      )
    {
        PyErr_SetString(PyExc_TypeError, "file must be a readable file-like"
            " object for COPY FROM; a writeable file-like object for COPY TO."
          );
        goto fail;
    }

    self->copysize = bufsize;
    self->copyfile = file;

    /* At this point, the SQL statement must be str, not unicode */
    if (pq_execute(self, PyString_AS_STRING(sql), 0) != 1) { goto fail; }

    res = Py_None;
    Py_INCREF(res);
    goto cleanup;
 fail:
    if (res != NULL) {
        Py_DECREF(res);
        res = NULL;
    }
    /* Fall through to cleanup */
 cleanup:
    self->copyfile = NULL;
    Py_XDECREF(sql);

    return res;
}

/* extension: closed - return true if cursor is closed*/

#define psyco_curs_closed_doc \
"True if cursor is closed, False if cursor is open"

static PyObject *
psyco_curs_get_closed(cursorObject *self, void *closure)
{
    PyObject *closed;

    closed = (self->closed || (self->conn && self->conn->closed)) ?
        Py_True : Py_False;
    Py_INCREF(closed);
    return closed;
}

#endif


/** the cursor object **/

/* iterator protocol */

static PyObject *
cursor_iter(PyObject *self)
{
    EXC_IF_CURS_CLOSED((cursorObject*)self);
    Py_INCREF(self);
    return self;
}

static PyObject *
cursor_next(PyObject *self)
{
    PyObject *res;

    /* we don't parse arguments: psyco_curs_fetchone will do that for us */
    res = psyco_curs_fetchone((cursorObject*)self, NULL);

    /* convert a None to NULL to signal the end of iteration */
    if (res && res == Py_None) {
        Py_DECREF(res);
        res = NULL;
    }
    return res;
}

/* object method list */

static struct PyMethodDef cursorObject_methods[] = {
    /* DBAPI-2.0 core */
    {"close", (PyCFunction)psyco_curs_close,
     METH_NOARGS, psyco_curs_close_doc},
    {"execute", (PyCFunction)psyco_curs_execute,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_execute_doc},
    {"executemany", (PyCFunction)psyco_curs_executemany,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_executemany_doc},
    {"fetchone", (PyCFunction)psyco_curs_fetchone,
     METH_NOARGS, psyco_curs_fetchone_doc},
    {"fetchmany", (PyCFunction)psyco_curs_fetchmany,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_fetchmany_doc},
    {"fetchall", (PyCFunction)psyco_curs_fetchall,
     METH_NOARGS, psyco_curs_fetchall_doc},
    {"callproc", (PyCFunction)psyco_curs_callproc,
     METH_VARARGS, psyco_curs_callproc_doc},
    {"nextset", (PyCFunction)psyco_curs_nextset,
     METH_NOARGS, psyco_curs_nextset_doc},
    {"setinputsizes", (PyCFunction)psyco_curs_setinputsizes,
     METH_VARARGS, psyco_curs_setinputsizes_doc},
    {"setoutputsize", (PyCFunction)psyco_curs_setoutputsize,
     METH_VARARGS, psyco_curs_setoutputsize_doc},
    /* DBAPI-2.0 extensions */
    {"scroll", (PyCFunction)psyco_curs_scroll,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_scroll_doc},
    /* psycopg extensions */
#ifdef PSYCOPG_EXTENSIONS
    {"mogrify", (PyCFunction)psyco_curs_mogrify,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_mogrify_doc},
    {"copy_from", (PyCFunction)psyco_curs_copy_from,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_copy_from_doc},
    {"copy_to", (PyCFunction)psyco_curs_copy_to,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_copy_to_doc},
    {"copy_expert", (PyCFunction)psyco_curs_copy_expert,
     METH_VARARGS|METH_KEYWORDS, psyco_curs_copy_expert_doc},
#endif
    {NULL}
};

/* object member list */

#define OFFSETOF(x) offsetof(cursorObject, x)

static struct PyMemberDef cursorObject_members[] = {
    /* DBAPI-2.0 basics */
    {"rowcount", T_LONG, OFFSETOF(rowcount), RO,
        "Number of rows read from the backend in the last command."},
    {"arraysize", T_LONG, OFFSETOF(arraysize), 0,
        "Number of records `fetchmany()` must fetch if not explicitly " \
        "specified."},
    {"description", T_OBJECT, OFFSETOF(description), RO,
        "Cursor description as defined in DBAPI-2.0."},
    {"lastrowid", T_LONG, OFFSETOF(lastoid), RO,
        "The ``oid`` of the last row inserted by the cursor."},
    /* DBAPI-2.0 extensions */
    {"rownumber", T_LONG, OFFSETOF(row), RO,
        "The current row position."},
    {"connection", T_OBJECT, OFFSETOF(conn), RO,
        "The connection where the cursor comes from."},
#ifdef PSYCOPG_EXTENSIONS
    {"name", T_STRING, OFFSETOF(name), RO},
    {"statusmessage", T_OBJECT, OFFSETOF(pgstatus), RO,
        "The return message of the last command."},
    {"query", T_OBJECT, OFFSETOF(query), RO,
        "The last query text sent to the backend."},
    {"row_factory", T_OBJECT, OFFSETOF(tuple_factory), 0},
    {"tzinfo_factory", T_OBJECT, OFFSETOF(tzinfo_factory), 0},
    {"typecaster", T_OBJECT, OFFSETOF(caster), RO},
    {"string_types", T_OBJECT, OFFSETOF(string_types), 0},
    {"binary_types", T_OBJECT, OFFSETOF(binary_types), 0},
#endif
    {NULL}
};

/* object calculated member list */
static struct PyGetSetDef cursorObject_getsets[] = {
#ifdef PSYCOPG_EXTENSIONS
    { "closed", (getter)psyco_curs_get_closed, NULL,
      psyco_curs_closed_doc, NULL },
#endif
    {NULL}
};

/* initialization and finalization methods */

static int
cursor_setup(cursorObject *self, connectionObject *conn, const char *name)
{
    Dprintf("cursor_setup: init cursor object at %p", self);
    Dprintf("cursor_setup: parameters: name = %s, conn = %p", name, conn);

    if (name) {
        self->name = PyMem_Malloc(strlen(name)+1);
        if (self->name == NULL) return 1;
        strncpy(self->name, name, strlen(name)+1);
    }

    /* FIXME: why does this raise an excpetion on the _next_ line of code?
    if (PyObject_IsInstance((PyObject*)conn,
                             (PyObject *)&connectionType) == 0) {
        PyErr_SetString(PyExc_TypeError,
            "argument 1 must be subclass of psycopg2._psycopg.connection");
        return 1;
    } */
    Py_INCREF(conn);
    self->conn = conn;

    self->closed = 0;
    self->mark = conn->mark;
    self->pgres = NULL;
    self->notuples = 1;
    self->arraysize = 1;
    self->rowcount = -1;
    self->lastoid = InvalidOid;

    self->casts = NULL;
    self->notice = NULL;

    self->string_types = NULL;
    self->binary_types = NULL;

    Py_INCREF(Py_None);
    self->description = Py_None;
    Py_INCREF(Py_None);
    self->pgstatus = Py_None;
    Py_INCREF(Py_None);
    self->tuple_factory = Py_None;
    Py_INCREF(Py_None);
    self->query = Py_None;

    /* default tzinfo factory */
    Py_INCREF(pyPsycopgTzFixedOffsetTimezone);
    self->tzinfo_factory = pyPsycopgTzFixedOffsetTimezone;

    Dprintf("cursor_setup: good cursor object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        self, ((PyObject *)self)->ob_refcnt
      );
    return 0;
}

static void
cursor_dealloc(PyObject* obj)
{
    cursorObject *self = (cursorObject *)obj;
    
    PyObject_GC_UnTrack(self);

    if (self->name) PyMem_Free(self->name);

    Py_CLEAR(self->conn);
    Py_CLEAR(self->casts);
    Py_CLEAR(self->description);
    Py_CLEAR(self->pgstatus);
    Py_CLEAR(self->tuple_factory);
    Py_CLEAR(self->tzinfo_factory);
    Py_CLEAR(self->query);
    Py_CLEAR(self->string_types);
    Py_CLEAR(self->binary_types);

    IFCLEARPGRES(self->pgres);

    Dprintf("cursor_dealloc: deleted cursor object at %p, refcnt = "
        FORMAT_CODE_PY_SSIZE_T,
        obj, obj->ob_refcnt);

    obj->ob_type->tp_free(obj);
}

static int
cursor_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    const char *name = NULL;
    PyObject *conn;

    if (!PyArg_ParseTuple(args, "O|s", &conn, &name))
        return -1;

    return cursor_setup((cursorObject *)obj, (connectionObject *)conn, name);
}

static PyObject *
cursor_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static void
cursor_del(PyObject* self)
{
    PyObject_GC_Del(self);
}

static PyObject *
cursor_repr(cursorObject *self)
{
    return PyString_FromFormat(
        "<cursor object at %p; closed: %d>", self, self->closed);
}

static int
cursor_traverse(cursorObject *self, visitproc visit, void *arg)
{
    Py_VISIT((PyObject *)self->conn);
    Py_VISIT(self->description);
    Py_VISIT(self->pgstatus);
    Py_VISIT(self->casts);
    Py_VISIT(self->caster);
    Py_VISIT(self->copyfile);
    Py_VISIT(self->tuple_factory);
    Py_VISIT(self->tzinfo_factory);
    Py_VISIT(self->query);
    Py_VISIT(self->string_types);
    Py_VISIT(self->binary_types);
    return 0;
}


/* object type */

#define cursorType_doc \
"A database cursor."

PyTypeObject cursorType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "psycopg2._psycopg.cursor",
    sizeof(cursorObject),
    0,
    cursor_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    (reprfunc)cursor_repr, /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */

    0,          /*tp_call*/
    (reprfunc)cursor_repr, /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/

    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_ITER |
      Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    cursorType_doc, /*tp_doc*/

    (traverseproc)cursor_traverse, /*tp_traverse*/
    0,          /*tp_clear*/

    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/

    cursor_iter, /*tp_iter*/
    cursor_next, /*tp_iternext*/

    /* Attribute descriptor and subclassing stuff */

    cursorObject_methods, /*tp_methods*/
    cursorObject_members, /*tp_members*/
    cursorObject_getsets, /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/

    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/

    cursor_init, /*tp_init*/
    0, /*tp_alloc  Will be set to PyType_GenericAlloc in module init*/
    cursor_new, /*tp_new*/
    (freefunc)cursor_del, /*tp_free  Low-level free-memory routine */
    0,          /*tp_is_gc For PyObject_IS_GC */
    0,          /*tp_bases*/
    0,          /*tp_mro method resolution order */
    0,          /*tp_cache*/
    0,          /*tp_subclasses*/
    0           /*tp_weaklist*/
};
