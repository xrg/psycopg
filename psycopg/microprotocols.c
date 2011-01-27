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
#include "psycopg/adapter_asis.h"


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

static int _psyco_float2bin(PyObject *obj, char** data, int* len, 
                            Oid* ptype, PyObject **obref, int* fmt,
                            connectionObject *conn);

static int _psyco_buf2bin(PyObject *obj, char** data, int* len, 
                            Oid* ptype, PyObject **obref, int* fmt,
                            connectionObject *conn);

static int _psyco_list2bin(PyObject *obj, char** data, int* len, 
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
    
    Dprintf("Initializing microprotocol fast addapters");
    psyco_py2bins = PyMem_New(microprotocols_py2bin, 10);
    memset(psyco_py2bins, '\0', sizeof(microprotocols_py2bin) * 10);
    microprotocols_addbin(&PyString_Type, NULL, _psyco_str2bin);
    microprotocols_addbin(&PyInt_Type, NULL, _psyco_int2bin);
    microprotocols_addbin(&PyLong_Type, NULL, _psyco_int2bin);
    microprotocols_addbin(&PyUnicode_Type, NULL, _psyco_ustr2bin);
    microprotocols_addbin(&PyFloat_Type, NULL, _psyco_float2bin);
    microprotocols_addbin(&PyBuffer_Type, NULL, _psyco_buf2bin);
    microprotocols_addbin(&PyList_Type, NULL, _psyco_list2bin);

    return 0;
}


/* microprotocols_add - add a reverse type-caster to the dictionary */

int
microprotocols_add(PyTypeObject *type, PyObject *proto, PyObject *cast)
{
    PyObject *key;

    if (proto == NULL) proto = (PyObject*)&isqlquoteType;

    Dprintf("microprotocols_add: cast %p for (%s, ?)", cast, type->tp_name);

    key = PyTuple_Pack(2, (PyObject*)type, proto);
    PyDict_SetItem(psyco_adapters, key, cast);
    Py_DECREF(key);

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


/* Check if one of `obj` superclasses has an adapter for `proto`.
 *
 * If it does, return a *borrowed reference* to the adapter, else NULL.
 */
static PyObject *
_get_superclass_adapter(PyObject *obj, PyObject *proto)
{
    PyTypeObject *type;
    PyObject *mro, *st;
    PyObject *key, *adapter;
    Py_ssize_t i, ii;

    type = (PyTypeObject *)Py_TYPE(obj);
    if (!((Py_TPFLAGS_HAVE_CLASS & type->tp_flags) && type->tp_mro)) {
        /* has no mro */
        return NULL;
    }

    /* Walk the mro from the most specific subclass. */
    mro = type->tp_mro;
    for (i = 1, ii = PyTuple_GET_SIZE(mro); i < ii; ++i) {
        st = PyTuple_GET_ITEM(mro, i);
        key = PyTuple_Pack(2, st, proto);
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
    return NULL;
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
       
    /* None is always adapted to NULL */
    
    if (obj == Py_None)
        return PyString_FromString("NULL");

    Dprintf("microprotocols_adapt: trying to adapt %s", obj->ob_type->tp_name);

    /* look for an adapter in the registry */
    key = PyTuple_Pack(2, Py_TYPE(obj), proto);
    adapter = PyDict_GetItem(psyco_adapters, key);
    Py_DECREF(key);
    if (adapter) {
        adapted = PyObject_CallFunctionObjArgs(adapter, obj, NULL);
        return adapted;
    }

    /* Check if a superclass can be adapted and use the same adapter. */
    if (NULL != (adapter = _get_superclass_adapter(obj, proto))) {
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
    PyOS_snprintf(buffer, 255, "can't adapt type '%s'", obj->ob_type->tp_name);
    psyco_set_error(ProgrammingError, NULL, buffer, NULL, NULL);
    return NULL;
}

/* microprotocol_getquoted - utility function that adapt and call getquoted */

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
            adapted->ob_type->tp_name);

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
    else if (obj && obj->ob_type == &PyTuple_Type)
        // Short circuit, avoid adaptation and call cycles below
        // to be removed, if tuples are ever supported in binary
        return -2;

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
            if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_NotImplementedError)){
                PyErr_Clear();
                return -2;
            }
            Dprintf("No %s.getraw() attribute provided", tmp->ob_type->tp_name);
            if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_AttributeError))
                PyErr_Format(PyExc_AttributeError, 
                             "method %s.getraw() not implemented",
                             tmp->ob_type->tp_name);
            return -1;
        }
        if (res == obj){
	    /* We have failed to convert obj earlier, so don't recurse ever
	    again on it! */
	    PyErr_Format(PyExc_AttributeError, 
                             "method %s.getraw() returns the object before adapt.",
                             tmp->ob_type->tp_name);
	    return -2;
	}
        Dprintf("getraw() on argument returned %s", res->ob_type->tp_name);
        ri = microprotocol_addparams(res, conn, pargs, index, nbuf, nlen);
	if (ri == 1  && pargs->paramFormats[index] == 0){
	    /* We should let the backend find out the type, because
	       we cannot tell where the tmp came from */
	    res = PyObject_CallMethod(tmp, "getraw_oid", NULL);
	    if (res == NULL || res == Py_None)
		pargs->paramTypes[index] = 0;
	    else if (res == Py_False) {
		/* don't change the oid */
	    }
	    else if (PyInt_Check(res))
		pargs->paramTypes[index] = (int) PyInt_AsLong(res);
	}
        Py_DECREF(tmp);
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
	Py_ssize_t slen;
	Py_INCREF(obj);
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
    PyObject *enc;

    /* if the wrapped object is an unicode object we can encode it to match
       self->encoding but if the encoding is not specified we don't know what
       to do and we raise an exception */

    if (!conn->encoding){
        PyErr_SetString(PyExc_TypeError,
			"missing encoding for unicode conversion");
        return -1;
    }
    
    Dprintf("_psyco_ustr2bin: encoding to %s", conn->encoding);

    enc = PyDict_GetItemString(psycoEncodings, conn->encoding);
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

int _psyco_float2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt,
			    connectionObject *conn){
	double dn = PyFloat_AsDouble(obj);
	uint32_t *ptr;
	*data = PyMem_Malloc(sizeof(double));
	ptr = (uint32_t *) *data;
	
	/* Note: we use pointer-level casting, because (uint) a_float will
	   implicitly convert the numeric value */
	if (sizeof(double) == 8){
	    *ptype = FLOAT8OID;
	    *ptr = htonl((uint32_t) ( *((uint64_t *)&dn) >> 32));
	    ptr++;
	}else
	    *ptype = FLOAT4OID;

	*ptr = htonl(*((uint32_t *)&dn));
	*len = sizeof(double);
	*fmt = 1;
	return 1;
}

int _psyco_buf2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt,
			    connectionObject *conn){
    
	Py_ssize_t blen;
	Py_INCREF(obj);
	if (PyObject_AsReadBuffer(obj, (const void **)data, &blen) < 0)
	    return -1;
	*ptype = BYTEAOID;
	*len = blen;
	*fmt = 1;
	*obRef = obj;
	return 1;
}


/** Serialize a python list to an one-dimensional pg array
*/
int _psyco_list2bin(PyObject *obj, char** data, int* len, 
			    Oid* ptype, PyObject **obRef, int* fmt,
			    connectionObject *conn){
    
	Py_ssize_t alen = 0L, buflen = 0L;
	unsigned long int i;
	int has_nulls = 0;
	struct pq_exec_args ourargs;
	Oid itemoid = 0;
	uint32_t *buf;
	PyObject *ito;
	int ri;
	/* First, try with the data */
	
	Py_INCREF(obj); /* assume this procedure will take some time */
	
	buflen = 16L + sizeof(Oid);
	
	_init_pargs(&ourargs);
	alen = PyList_GET_SIZE(obj);
	if (alen >= INT_MAX)
	    return -1;
	_resize_pargs(&ourargs, alen);
	
	Dprintf("Resized to %ld args %d", alen, ourargs.nParams);
	
	for(i=0; i< alen; i++){
	    ito = PyList_GetItem(obj, i);
	    if (ito == NULL)
		goto fail;
	    if ((ri = microprotocol_addparams(ito, conn, &ourargs, i, NULL, NULL)) < 0)
                goto fail;

	    if (ri != 1){
		PyErr_Format(PyExc_AttributeError, "Cannot serialize %s element from array",
			     ito->ob_type->tp_name);
		goto fail;
	    }

	    if (itemoid && ourargs.paramTypes[i] && (ourargs.paramTypes[i] != itemoid)){
		PyErr_Format(PyExc_AttributeError, "Invalid %s element in array",
			     ito->ob_type->tp_name);
		goto fail;
	    }
	    if (ourargs.paramValues[i] && (ourargs.paramFormats[i] != 1)){
		PyErr_Format(PyExc_AttributeError, 
			     "Cannot serialize text for %s in binary array",
			     ito->ob_type->tp_name);
		goto fail;
	    }
	    if (!itemoid)
		itemoid = ourargs.paramTypes[i];

	    buflen += ourargs.paramLengths[i] + 4;
	    ito = NULL;
	}
	
	/* Now, we have buffers for all items, iterate once more and
	serialize them into one big buffer
	*/
	*data = PyMem_Malloc(buflen);
	buf = (uint32_t*) *data;
	if (! *data)
	    goto fail;
	
	*(buf++) = htonl(1); /* =ndims */
	*(buf++) = htonl(has_nulls);
	Dprintf("sizeof(Oid) = %ld", sizeof(Oid));

	*((Oid*)buf) = htonl(itemoid); // will it work for 
	buf += sizeof(Oid)/4;
	*(buf++) = htonl(alen); /* dim[0] */
	*(buf++) = htonl(0); /* lbound[0] */
	
	for (i=0;i<alen; i++){
	    if (!ourargs.paramValues[i])
		*(buf++) = htonl(-1);
	    else{
		*(buf++) = htonl(ourargs.paramLengths[i]);
		memcpy(buf,ourargs.paramValues[i],ourargs.paramLengths[i]);
		buf += ourargs.paramLengths[i] / 4;
	    }
		
	    if (ourargs.obRefs[i]){
		Py_DECREF(ourargs.obRefs[i]);
		ourargs.obRefs[i] = NULL;
	    }else
		PyMem_Free(ourargs.paramValues[i]);
	    ourargs.paramValues[i] = NULL;
	}
	
	if (( ((char*)buf) - *data) != buflen){
	    Dprintf("I smell fish! buflen: %ld offset: %ld", buflen,
		    (((char*)buf) - *data));
	}
	
	switch(itemoid){
	    
	    case TEXTOID: *ptype = TEXTARRAYOID; break;
	    case INT4OID: *ptype = INT4ARRAYOID; break;
	    //case INT8OID: *ptype = INT8ARRAYOID; break;
	    //case BOOLOID: *ptype = BOOL ARRAYOID; break;
	    case FLOAT4OID: *ptype = FLOAT4ARRAYOID; break;
	    //case INT4OID: *ptype = INT4ARRAYOID; break;
	    default:
		Dprintf("Could not get a solid array type for oid %d", itemoid);
		*ptype = ANYARRAYOID;
		goto fail; // TODO: until we make anyarray work..
	}
	*len = buflen;
	*fmt = 1;
	*obRef = NULL;
	_free_pargs(&ourargs);
	Py_DECREF(obj);
	return 1;
	
	fail:
	for (i=0L; i<alen; i++){
	    if (ourargs.obRefs[i]){
		Py_DECREF(ourargs.obRefs[i]);
		ourargs.obRefs[i] = NULL;
	    }else
		PyMem_Free(ourargs.paramValues[i]);
	    ourargs.paramValues[i] = NULL;
	}
	_free_pargs(&ourargs);
	Py_DECREF(obj);
	return -1;
}

/*eof*/