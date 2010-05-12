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

#define PSYCOPG_MODULE
#include "psycopg/psycopg.h"

#include "psycopg/microprotocols.h"
#include "psycopg/microprotocols_proto.h"
#include "psycopg/cursor.h"
#include "psycopg/adapter_asis.h"
#include "psycopg/connection.h"


#include "pgtypes.h"

/** the adapters registry **/

PyObject *psyco_adapters;

/** the fast-path py2bin registry */
microprotocols_py2bin *psyco_py2bins = 0;

static int _psyco_str2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obref, int* fmt,
			    connectionObject *conn);

static int _psyco_ustr2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obref, int* fmt,
			    connectionObject *conn);

static int _psyco_int2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obref, int* fmt,
			    connectionObject *conn);

static int _psyco_long2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obref, int* fmt,
			    connectionObject *conn);

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
    microprotocols_addbin(&PyUnicode_Type, NULL, _psyco_ustr2bin);

    return 0;
}


/* microprotocols_add - add a reverse type-caster to the dictionary
 *
 * Return 0 on success, else -1 and set an exception.
 */
int
microprotocols_add(PyTypeObject *type, PyObject *proto, PyObject *cast)
{
    PyObject *key = NULL;
    int rv = -1;

    if (proto == NULL) proto = (PyObject*)&isqlquoteType;

    Dprintf("microprotocols_add: cast %p for (%s, ?)", cast, type->tp_name);

    if (!(key = PyTuple_Pack(2, (PyObject*)type, proto))) { goto exit; }
    if (0 != PyDict_SetItem(psyco_adapters, key, cast)) { goto exit; }

    rv = 0;

exit:
    Py_XDECREF(key);
    return rv;
}

/* Check if one of `obj` superclasses has an adapter for `proto`.
 *
 * If it does, return a *borrowed reference* to the adapter, else to None.
 */
BORROWED static PyObject *
_get_superclass_adapter(PyObject *obj, PyObject *proto)
{
    PyTypeObject *type;
    PyObject *mro, *st;
    PyObject *key, *adapter;
    Py_ssize_t i, ii;

    type = Py_TYPE(obj);
    if (!(
#if PY_MAJOR_VERSION < 3
        (Py_TPFLAGS_HAVE_CLASS & type->tp_flags) &&
#endif
        type->tp_mro)) {
        /* has no mro */
        return Py_None;
    }

    /* Walk the mro from the most specific subclass. */
    mro = type->tp_mro;
    for (i = 1, ii = PyTuple_GET_SIZE(mro); i < ii; ++i) {
        st = PyTuple_GET_ITEM(mro, i);
        if (!(key = PyTuple_Pack(2, st, proto))) { return NULL; }
        adapter = PyDict_GetItem(psyco_adapters, key);
        Py_DECREF(key);

        if (adapter) {
            Dprintf(
                "microprotocols_adapt: using '%s' adapter to adapt '%s'",
                ((PyTypeObject *)st)->tp_name, type->tp_name);

            /* register this adapter as good for the subclass too,
             * so that the next time it will be found in the fast path */

            /* Well, no, maybe this is not a good idea.
             * It would become a leak in case of dynamic
             * classes generated in a loop (think namedtuples). */

            /* key = PyTuple_Pack(2, (PyObject*)type, proto);
             * PyDict_SetItem(psyco_adapters, key, adapter);
             * Py_DECREF(key);
             */
            return adapter;
        }
    }
    return Py_None;
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
    PyObject *adapter, *adapted, *key, *meth;
    char buffer[256];

    /* we don't check for exact type conformance as specified in PEP 246
       because the ISQLQuote type is abstract and there is no way to get a
       quotable object to be its instance */

    Dprintf("microprotocols_adapt: trying to adapt %s",
        Py_TYPE(obj)->tp_name);

    /* look for an adapter in the registry */
    if (!(key = PyTuple_Pack(2, Py_TYPE(obj), proto))) { return NULL; }
    adapter = PyDict_GetItem(psyco_adapters, key);
    Py_DECREF(key);
    if (adapter) {
        adapted = PyObject_CallFunctionObjArgs(adapter, obj, NULL);
        return adapted;
    }

    /* Check if a superclass can be adapted and use the same adapter. */
    if (!(adapter = _get_superclass_adapter(obj, proto))) {
        return NULL;
    }
    if (Py_None != adapter) {
        adapted = PyObject_CallFunctionObjArgs(adapter, obj, NULL);
        return adapted;
    }

    /* try to have the protocol adapt this object*/
    if ((meth = PyObject_GetAttrString(proto, "__adapt__"))) {
        adapted = PyObject_CallFunctionObjArgs(meth, obj, NULL);
        Py_DECREF(meth);
        if (adapted && adapted != Py_None) return adapted;
        Py_XDECREF(adapted);
        if (PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_TypeError)) {
               PyErr_Clear();
            } else {
                return NULL;
            }
        }
    }
    else {
        /* proto.__adapt__ not found. */
        PyErr_Clear();
    }

    /* and finally try to have the object adapt itself */
    if ((meth = PyObject_GetAttrString(obj, "__conform__"))) {
        adapted = PyObject_CallFunctionObjArgs(meth, proto, NULL);
        Py_DECREF(meth);
        if (adapted && adapted != Py_None) return adapted;
        Py_XDECREF(adapted);
        if (PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_TypeError)) {
               PyErr_Clear();
            } else {
                return NULL;
            }
        }
    }
    else {
        /* obj.__conform__ not found. */
        PyErr_Clear();
    }

    /* else set the right exception and return NULL */
    PyOS_snprintf(buffer, 255, "can't adapt type '%s'",
        Py_TYPE(obj)->tp_name);
    psyco_set_error(ProgrammingError, NULL, buffer, NULL, NULL);
    return NULL;
}

/* microprotocol_getquoted - utility function that adapt and call getquoted.
 *
 * Return a bytes string, NULL on error.
 */

PyObject *
microprotocol_getquoted(PyObject *obj, connectionObject *conn)
{
    PyObject *res = NULL;
    PyObject *prepare = NULL;
    PyObject *adapted;

    if (!(adapted = microprotocols_adapt(obj, (PyObject*)&isqlquoteType, NULL))) {
       goto exit;
    }

    Dprintf("microprotocol_getquoted: adapted to %s",
        Py_TYPE(adapted)->tp_name);

    /* if requested prepare the object passing it the connection */
    if (conn) {
        if ((prepare = PyObject_GetAttrString(adapted, "prepare"))) {
            res = PyObject_CallFunctionObjArgs(
                prepare, (PyObject *)conn, NULL);
            if (res) {
                Py_DECREF(res);
                res = NULL;
            } else {
                goto exit;
            }
        }
        else {
            /* adapted.prepare not found */
            PyErr_Clear();
        }
    }

    /* call the getquoted method on adapted (that should exist because we
       adapted to the right protocol) */
    res = PyObject_CallMethod(adapted, "getquoted", NULL);

    /* Convert to bytes. */
    if (res && PyUnicode_CheckExact(res)) {
        PyObject *b;
        const char *codec;
        codec = (conn && conn->codec) ? conn->codec : "utf8";
        b = PyUnicode_AsEncodedString(res, codec, NULL);
        Py_DECREF(res);
        res = b;
    }

exit:
    Py_XDECREF(adapted);
    Py_XDECREF(prepare);

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
                        &pargs->obRefs[index], &pargs->paramFormats[index], conn);
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
                        &pargs->obRefs[index], &pargs->paramFormats[index], conn);
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
        if (res == NULL){
            Dprintf("No %s.getraw() attribute provided", tmp->ob_type->tp_name);
            return -1;
        }
        Dprintf("getraw() on argument returned %s", res->ob_type->tp_name);
        Py_DECREF(tmp);
        ri = microprotocol_addparams(res, conn, pargs, index, nbuf, nlen);
	if (ri == 1  && pargs->paramFormats[index] == 0){
	    /* We should let the backend find out the type, because
	       we cannot tell where the tmp came from */
	    pargs->paramTypes[index] = 0;
	}
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
			    Oid* ptype, PyObject **obRef, int* fmt, 
			    connectionObject *conn){
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
			    Oid* ptype, PyObject **obRef, int* fmt,
			    connectionObject *conn){
	*data = PyMem_Malloc(sizeof(int32_t));
	*((int32_t *) *data) = htonl(PyInt_AsLong(obj));
	*ptype = INT4OID;
	*len = sizeof(int32_t);
	*fmt = 1;
	return 1;
}

int _psyco_long2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt,
			    connectionObject *conn){
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

/** Try to take encoded data of unicode string
    This time it's not that easy, since we have to convert to the native
    encoding of the psycopg connection. But still, we do the minimal 
    possible byte and buffer usage.

*/

int _psyco_ustr2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt,
			    connectionObject *conn){
    
    PyObject *str;
    Py_ssize_t slen;

    /* if the wrapped object is an unicode object we can encode it to match
       self->encoding but if the encoding is not specified we don't know what
       to do and we raise an exception */

    if (!conn->encoding){
        PyErr_SetString(PyExc_TypeError,
			"missing encoding for unicode conversion");
        return -1;
    }
    
    Dprintf("_psyco_ustr2bin: encoding to %s", conn->encoding);

    PyObject *enc = PyDict_GetItemString(psycoEncodings, conn->encoding);
    /* note that enc is a borrowed reference */

    if (enc) {
	const char *s = PyString_AsString(enc);
	Dprintf("_psyco_ustr2bin: encoding unicode object to %s", s);
	str = PyUnicode_AsEncodedString(obj, s, NULL);
	Dprintf("_psyco_ustr2bin: got encoded object at %p", str);
	if (str == NULL) return -1;
    }
    else {
	/* can't find the right encoder, raise exception */
	PyErr_Format(InterfaceError,
			"can't encode unicode string to %s", conn->encoding);
	return -1;
    }

    Py_INCREF(str);
    PyString_AsStringAndSize(str, data, &slen);
    *len = slen; /* we don't handle more than 3GB here */
    *ptype = VARCHAROID;
    *fmt = 1;
    *obRef = str;
    return 1;
}

/*eof*/