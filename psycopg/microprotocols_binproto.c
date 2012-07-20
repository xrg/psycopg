/* microprotocol_proto.c - psycopg protocols
 *
 * Copyright (C) 2003-2010 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2012 Panos Christeas <xrg@hellug.gr>
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
#include "psycopg/microprotocols_binproto.h"

#include <string.h>
#include "pgtypes.h"



/** void protocol implementation **/


/* getquoted - return quoted representation for object */

#define psyco_isqlparam_getraw_doc \
"getraw() -- return SQL-buffer representation of this object"

static PyObject *
psyco_isqlparam_getraw(isqlparamObject *self, PyObject *args)
{
    Py_INCREF(Py_None);
    return Py_None;
}

/* getbinary - return quoted representation for object */

#define psyco_isqlparam_getraw_oid_doc \
"getbinary() -- return SQL-quoted binary representation of this object"

static PyObject *
psyco_isqlparam_getraw_oid(isqlparamObject *self, PyObject *args)
{
    Py_INCREF(Py_False);
    return Py_False;
}

#define psyco_isqlparam_getbuffer_doc \
"getbuffer() -- return this object"

static PyObject *
psyco_isqlparam_getbuffer(isqlparamObject *self, PyObject *args)
{
    Py_INCREF(Py_None);
    return Py_None;
}

/** the ISQLParam object **/


/* object method list */

static struct PyMethodDef isqlparamObject_methods[] = {
    {"getraw", (PyCFunction)psyco_isqlparam_getraw,
     METH_NOARGS, psyco_isqlparam_getraw_doc},
    {"getraw_oid", (PyCFunction)psyco_isqlparam_getraw_oid,
     METH_NOARGS, psyco_isqlparam_getraw_oid_doc},
    {"getbuffer", (PyCFunction)psyco_isqlparam_getbuffer,
     METH_NOARGS, psyco_isqlparam_getbuffer_doc},
    /*    {"prepare", (PyCFunction)psyco_isqlparam_prepare,
          METH_VARARGS, psyco_isqlparam_prepare_doc}, */
    {NULL}
};

/* object member list */

static struct PyMemberDef isqlparamObject_members[] = {
    /* DBAPI-2.0 extensions (exception objects) */
    {"_wrapped", T_OBJECT, offsetof(isqlparamObject, wrapped), READONLY},
    {NULL}
};

/* initialization and finalization methods */

static int
isqlparam_setup(isqlparamObject *self, PyObject *wrapped)
{
    self->wrapped = wrapped;
    Py_INCREF(wrapped);

    return 0;
}

static void
isqlparam_dealloc(PyObject* obj)
{
    isqlparamObject *self = (isqlparamObject *)obj;

    Py_XDECREF(self->wrapped);

    Py_TYPE(obj)->tp_free(obj);
}

static int
isqlparam_init(PyObject *obj, PyObject *args, PyObject *kwds)
{
    PyObject *wrapped = NULL;

    if (!PyArg_ParseTuple(args, "O", &wrapped))
        return -1;

    return isqlparam_setup((isqlparamObject *)obj, wrapped);
}

static PyObject *
isqlparam_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc(type, 0);
}

static void
isqlparam_del(PyObject* self)
{
    PyObject_Del(self);
}


/* object type */

#define isqlparamType_doc \
"Abstract ISQLParam protocol\n\n" \
"An object conform to this protocol should expose a ``getraw()`` method\n" \
"returning the SQL binary representation of the object.\n\n"

PyTypeObject isqlparamType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2._psycopg.ISQLParam",
    sizeof(isqlparamObject),
    0,
    isqlparam_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    0,          /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    0,          /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash */

    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/

    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
    isqlparamType_doc, /*tp_doc*/

    0,          /*tp_traverse*/
    0,          /*tp_clear*/

    0,          /*tp_richcompare*/
    0,          /*tp_weaklistoffset*/

    0,          /*tp_iter*/
    0,          /*tp_iternext*/

    /* Attribute descriptor and subclassing stuff */

    isqlparamObject_methods, /*tp_methods*/
    isqlparamObject_members, /*tp_members*/
    0,          /*tp_getset*/
    0,          /*tp_base*/
    0,          /*tp_dict*/

    0,          /*tp_descr_get*/
    0,          /*tp_descr_set*/
    0,          /*tp_dictoffset*/

    isqlparam_init, /*tp_init*/
    0, /*tp_alloc  will be set to PyType_GenericAlloc in module init*/
    isqlparam_new, /*tp_new*/
    (freefunc)isqlparam_del, /*tp_free  Low-level free-memory routine */
    0,          /*tp_is_gc For PyObject_IS_GC */
    0,          /*tp_bases*/
    0,          /*tp_mro method resolution order */
    0,          /*tp_cache*/
    0,          /*tp_subclasses*/
    0           /*tp_weaklist*/
};

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


void microprotocols_bin_init(){
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
        obj, (PyObject*)&isqlparamType, NULL);

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
    const char *s;

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
	s = PyString_AsString(enc);
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