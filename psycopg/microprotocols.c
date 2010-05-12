/* microprotocols.c - minimalist and non-validating protocols implementation
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

#define PSYCOPG_MODULE
#include "psycopg/config.h"
#include "psycopg/python.h"
#include "psycopg/psycopg.h"
#include "psycopg/cursor.h"
#include "psycopg/connection.h"
#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"


#include "pgtypes.h"

/** the adapters registry **/

PyObject *psyco_adapters;

/** the fast-path py2bin registry */
microprotocols_py2bin *psyco_py2bins = 0;

static int _psyco_str2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obref, int* fmt );

static int _psyco_int2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obref, int* fmt );

static int _psyco_long2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obref, int* fmt );

/* microprotocols_init - initialize the adapters dictionary */

int
microprotocols_init(PyObject *dict)
{
    /* create adapters dictionary and put it in module namespace */
    if ((psyco_adapters = PyDict_New()) == NULL) {
        return -1;
    }

    PyDict_SetItemString(dict, "adapters", psyco_adapters);
    
    psyco_py2bins = PyMem_New(microprotocols_py2bin, 10);
    memset(psyco_py2bins, '\0', sizeof(microprotocols_py2bin) * 10);
    microprotocols_addbin(&PyString_Type, NULL, _psyco_str2bin);
    microprotocols_addbin(&PyInt_Type, NULL, _psyco_int2bin);
    microprotocols_addbin(&PyLong_Type, NULL, _psyco_int2bin);
    

    return 0;
}


/* microprotocols_add - add a reverse type-caster to the dictionary */

int
microprotocols_add(PyTypeObject *type, PyObject *proto, PyObject *cast)
{
    if (proto == NULL) proto = (PyObject*)&isqlquoteType;

    Dprintf("microprotocols_add: cast %p for (%s, ?)", cast, type->tp_name);

    PyDict_SetItem(psyco_adapters,
                   Py_BuildValue("(OO)", (PyObject*)type, proto),
                   cast);
    return 0;
}

/** Add one type to the fast-path registry */
int microprotocols_addbin(PyTypeObject * pyType, psyco_checkfn checkFn,
                                 psyco_py2bin convFn){
    microprotocols_py2bin *p2b;
    for (p2b = psyco_py2bins; p2b->pyType ; p2b++);
    p2b->pyType = pyType;
    p2b->checkFn = checkFn;
    p2b->convFn = convFn;
}


/* microprotocols_adapt - adapt an object to the built-in protocol */

PyObject *
microprotocols_adapt(PyObject *obj, PyObject *proto, PyObject *alt)
{
    PyObject *adapter, *key;
    char buffer[256];

    /* we don't check for exact type conformance as specified in PEP 246
       because the ISQLQuote type is abstract and there is no way to get a
       quotable object to be its instance */
       
    /* None is always adapted to NULL */
    
    if (obj == Py_None)
        return PyString_FromString("NULL");

    Dprintf("microprotocols_adapt: trying to adapt %s", obj->ob_type->tp_name);

    /* look for an adapter in the registry */
    key = Py_BuildValue("(OO)", (PyObject*)obj->ob_type, proto);
    adapter = PyDict_GetItem(psyco_adapters, key);
    Py_DECREF(key);
    if (adapter) {
        PyObject *adapted = PyObject_CallFunctionObjArgs(adapter, obj, NULL);
        return adapted;
    }

    /* try to have the protocol adapt this object*/
    if (PyObject_HasAttrString(proto, "__adapt__")) {
        PyObject *adapted = PyObject_CallMethod(proto, "__adapt__", "O", obj);
        if (adapted && adapted != Py_None) return adapted;
        Py_XDECREF(adapted);
        if (PyErr_Occurred() && !PyErr_ExceptionMatches(PyExc_TypeError))
            return NULL;
    }

    /* and finally try to have the object adapt itself */
    if (PyObject_HasAttrString(obj, "__conform__")) {
        PyObject *adapted = PyObject_CallMethod(obj, "__conform__","O", proto);
        if (adapted && adapted != Py_None) return adapted;
        Py_XDECREF(adapted);
        if (PyErr_Occurred() && !PyErr_ExceptionMatches(PyExc_TypeError))
            return NULL;
    }

    /* else set the right exception and return NULL */
    PyOS_snprintf(buffer, 255, "can't adapt type '%s'", obj->ob_type->tp_name);
    psyco_set_error(ProgrammingError, NULL, buffer, NULL, NULL);
    return NULL;
}

/* microprotocol_getquoted - utility function that adapt and call getquoted */

PyObject *
microprotocol_getquoted(PyObject *obj, connectionObject *conn)
{
    PyObject *res = NULL;
    PyObject *tmp = microprotocols_adapt(
        obj, (PyObject*)&isqlquoteType, NULL);

    if (tmp != NULL) {
        Dprintf("microprotocol_getquoted: adapted to %s",
                tmp->ob_type->tp_name);

        /* if requested prepare the object passing it the connection */
        if (PyObject_HasAttrString(tmp, "prepare") && conn) {
            res = PyObject_CallMethod(tmp, "prepare", "O", (PyObject*)conn);
            if (res == NULL) {
                Py_DECREF(tmp);
                return NULL;
            }
            else {
                Py_DECREF(res);
            }
        }

        /* call the getquoted method on tmp (that should exist because we
           adapted to the right protocol) */
        res = PyObject_CallMethod(tmp, "getquoted", NULL);
        Py_DECREF(tmp);
    }

    /* we return res with one extra reference, the caller shall free it */
    return res;
}


/** Convert obj to raw data value for PQExecParams
    @param obj the variable to get data from
    @param pargs a structure with all arrays for PQExecparams
    @param index the position of the parameter in pargs
    @param nbuf a pointer to a string buffer for SQL expression
    @param nlen the resulting length of the sql expression
    
    @param return -1 on error, 0 on NO arg eg. AsIS(NULL), 1 when
                one arg must be added (nlen can be 0 here) or positive
                when multiple args must be added and nbuf/nlen have
                to be appended to the query string
    
    For most data types, their raw data should be added to the exec
    params, in the fastest possible path (that is, no conversions).
    For tuple, we may want the "($1, $2,...)" syntax, which needs
    the query string to be updated. There, we \b allocate @c nbuf and
    format the parentheses part in there, return the resulting nbuf
    length in @c nlen.

    @note that for complex data, we may call this function \b recursively !
*/
int
microprotocol_addparams(PyObject *obj, connectionObject *conn, 
	struct pq_exec_args *pargs, int index, char** nbuf, int* nlen)
{
    PyObject *res = NULL;
    PyObject *tmp = NULL;
    int ri = 0 ;
    Py_ssize_t len;
    microprotocols_py2bin *p2b;
    
    
    if (obj == Py_None){
        pargs->paramValues[index] = NULL;
        pargs->paramLengths[index] = 0;
        pargs->paramFormats[index] = 0;
        Dprintf("output Null at [%d] .", index);
        return 1;
    }
    else if ( obj == Py_True || obj == Py_False)
        return _psyco_bool2bin(obj, pargs->paramValues+index,
                        pargs->paramLengths+index, &pargs->paramTypes[index],
                        &pargs->obRefs[index], &pargs->paramFormats[index]);

    for (p2b = psyco_py2bins; p2b->pyType ; p2b++)
        if (p2b->pyType == obj->ob_type){
            ri = p2b->convFn(obj, pargs->paramValues+index,
                        pargs->paramLengths+index, &pargs->paramTypes[index],
                        &pargs->obRefs[index], &pargs->paramFormats[index]);
            if (ri < 0)
                break; /* assuming none of the other out args has been set */
            Dprintf("Adapted [%d] %s by object type: %d ",index, obj->ob_type->tp_name, ri);
            return ri;
        }
        
    /* try again with the check function */
    for (p2b = psyco_py2bins; p2b->pyType ; p2b++)
        if (p2b->checkFn && (p2b->checkFn)(obj)){
            ri = p2b->convFn(obj, &pargs->paramValues[index],
                        &pargs->paramLengths[index], &pargs->paramTypes[index],
                        &pargs->obRefs[index], &pargs->paramFormats[index]);
            if (ri < 0)
                break; /* assuming none of the other out args has been set */
            Dprintf("Adapted %s by object check ", obj->ob_type->tp_name);
            return ri;
        }
    
    tmp = microprotocols_adapt(
        obj, (PyObject*)&isqlquoteType, NULL);

    if (tmp != NULL) {
        Dprintf("microprotocol_getquoted: adapted %s to %s",
                obj->ob_type->tp_name, tmp->ob_type->tp_name);

        /* if requested prepare the object passing it the connection */
        if (PyObject_HasAttrString(tmp, "prepare") && conn) {
            res = PyObject_CallMethod(tmp, "prepare", "O", (PyObject*)conn);
            if (res == NULL) {
                Py_DECREF(tmp);
                Dprintf("prepare failed!");
                return -1;
            }
            else {
                Py_DECREF(res);
            }
        }

        /* call the getraw method on tmp (that should exist because we
           adapted to the right protocol) */
        
        res = PyObject_CallMethod(tmp, "getraw", NULL);
        if (res == NULL)
            return -1;
        Dprintf("getraw() on argument returned %s", res->ob_type->tp_name);
        Py_DECREF(tmp);
        ri = microprotocol_addparams(res, conn, pargs, index, nbuf, nlen);
    }

    
    /* we return res with one extra reference, the caller shall free it */
    return ri;
}

/** module-level functions **/

PyObject *
psyco_microprotocols_adapt(cursorObject *self, PyObject *args)
{
    PyObject *obj, *alt = NULL;
    PyObject *proto = (PyObject*)&isqlquoteType;

    if (!PyArg_ParseTuple(args, "O|OO", &obj, &proto, &alt)) return NULL;
    return microprotocols_adapt(obj, proto, alt);
}


int _psyco_str2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt ){
	Py_INCREF(obj);
	Py_ssize_t slen;
	PyString_AsStringAndSize(obj, data, &slen);
	*len = slen; /* we don't handle more than 3GB here */
	*ptype = VARCHAROID;
	*fmt = 0;
	*obRef = obj;
	return 1;
}

int _psyco_int2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt ){
	*data = PyMem_Malloc(sizeof(int32_t));
	*((int32_t *) *data) = htonl(PyInt_AsLong(obj));
	*ptype = INT4OID;
	*len = sizeof(int32_t);
	*fmt = 1;
	return 1;
}

int _psyco_long2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt ){
	*data = PyMem_Malloc(sizeof(int64_t));
	*((int64_t *) *data) = htonl(PyLong_AsLong(obj));
	*ptype = INT8OID;
	*len = sizeof(int64_t);
	*fmt = 1;
	return 1;
}

int _psyco_bool2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt ){
	*data = PyMem_Malloc(1);
	if (obj  == Py_True)
	    *((char*) *data)  = 1;
	else
	    *((char*) *data)  = 0;
	*ptype = BOOLOID;
	*len = 1;
	*fmt = 1;
	return 1;
}

/*eof*/