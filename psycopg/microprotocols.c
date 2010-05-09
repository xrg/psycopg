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


/** the adapters registry **/

PyObject *psyco_adapters;

/* microprotocols_init - initialize the adapters dictionary */

int
microprotocols_init(PyObject *dict)
{
    /* create adapters dictionary and put it in module namespace */
    if ((psyco_adapters = PyDict_New()) == NULL) {
        return -1;
    }

    PyDict_SetItemString(dict, "adapters", psyco_adapters);

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
    int ri = 0;
    Py_ssize_t len;
    
    if (obj == Py_None){
	pargs->paramValues[index] = NULL;
	pargs->paramLengths[index] = 0;
	pargs->paramFormats[index] = 0;
	Dprintf("output Null at [%d] .", index);
	return 1;
    }
    else if (PyString_Check/*Exact*/(obj)){
	PyString_AsStringAndSize(obj, &(pargs->paramValues[index]), &len);
	Dprintf("output string at [%d]%p %.10s..", index, pargs->paramValues[index], pargs->paramValues[index]);
	pargs->paramLengths[index] = len; // 32->64 bit truncate
	pargs->paramFormats[index] = 0;
	pargs->intRefs[index] = 0;
	return 1;
    }
    /*else if (PyInt_Check(obl)){
	pargs->paramValues[index] = 
    }*/
    
    tmp = microprotocols_adapt(
        obj, (PyObject*)&isqlquoteType, NULL);

    if (tmp != NULL) {
        Dprintf("microprotocol_getquoted: adapted to %s",
                tmp->ob_type->tp_name);

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
        Dprintf("getraw() on argument returned %s", res->ob_type->tp_name);
        if (res == NULL)
            res = Py_None;
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
