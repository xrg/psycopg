/* cursor_type.c - python interface to cursor objects
 *
 * Copyright (C) 2003-2010 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2010-2012 Panos Christeas <xrg@hellug.gr>
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

#define PSYCOPG_MODULE
#include "psycopg/psycopg.h"

#include "psycopg/cursor.h"
#include "psycopg/connection.h"
// #include "psycopg/green.h"
#include "psycopg/pqpath.h"
#include "psycopg/typecast.h"
#include "psycopg/microprotocols.h"
// #include "psycopg/microprotocols_proto.h"

#include <string.h>

#include <stdlib.h>

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
                        i = sprintf(rs,"$%d",oindex);
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

#define HAVE_EXECPARAMS 1

static int
_psyco_curs_execute2(cursorObject *self,
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
        /* if PyString_Format() return NULL an error occured: if the error is
           a TypeError we need to check the exception.args[0] string for the
           values:

               "not enough arguments for format string"
               "not all arguments converted"

           and return the appropriate ProgrammingError. we do that by grabbing
           the curren exception (we will later restore it if the type or the
           strings do not match.) */

        if (!(fquery = PyString_Format(operation, cvt))) {
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
