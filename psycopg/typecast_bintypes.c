/* pgcasts_basic.c - basic typecasting functions to python types
 *
 * Copyright (C) 2014 P. Christeas <xrg@hellug.gr>
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

#include <arpa/inet.h>
#include <sys/param.h>
 
uint64_t ntohll(uint64_t n)
{
#if __BYTE_ORDER == __BIG_ENDIAN
    return n;
#else
    return (((uint64_t)htonl(n)) << 32) + htonl(n >> 32);
#endif
}

static PyObject *
bincast_INT4_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    if (s == NULL) { Py_RETURN_NONE; }
    uint32_t i = *(uint32_t*) s;
    return PyInt_FromLong((int32_t)ntohl(i));
}

static PyObject *
bincast_INT8_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    if (s == NULL) { Py_RETURN_NONE; }
    uint64_t i = *(uint64_t*) s;
    return PyLong_FromUnsignedLongLong(ntohll(i));
}

static PyObject *
bincast_BOOLEAN_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    PyObject * res;
    if (s == NULL) { Py_RETURN_NONE; }
    if (s[0])
        res = Py_True;
    else
        res = Py_False;

    Py_INCREF(res);
    return res;
}

static PyObject *
bincast_FLOAT4_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    PyObject * res;
    if (s == NULL) { Py_RETURN_NONE; }
    
    return PyFloat_FromDouble(0.1);
}

static PyObject *
bincast_FLOAT8_cast(const char *s, Py_ssize_t len, PyObject *curs)
{
    PyObject * res;
    if (s == NULL) { Py_RETURN_NONE; }
    
    return PyFloat_FromDouble(0.1);
}


static typecastObject_initlist bincast_builtins[] = {
  {"BOOLEAN", (long int[]) {16 ,0}, bincast_BOOLEAN_cast, NULL},
  {"INTEGER", (long int[]) {23, 0}, bincast_INT4_cast, NULL},
  {"LONGINTEGER", (long int[]) {20, 0}, bincast_INT4_cast, NULL},
  {"STRING", (long int[]) {19, 18, 25, 1042, 1043, 0}, typecast_STRING_cast, NULL},
  {"FLOAT",(long int[]) {700, 0} , bincast_FLOAT4_cast, NULL},
  {"DOUBLE",(long int[]) {701, 0} , bincast_FLOAT8_cast, NULL},
  {"NUMERIC",(long int[]) {1700, 0} , bincast_FLOAT8_cast, NULL},
/*
  {"NUMBER", typecast_NUMBER_types, typecast_NUMBER_cast, NULL},
  {"LONGINTEGER", typecast_LONGINTEGER_types, typecast_LONGINTEGER_cast, NULL},
  {"INTEGER", typecast_INTEGER_types, typecast_INTEGER_cast, NULL},
  
  {"DECIMAL", typecast_DECIMAL_types, typecast_DECIMAL_cast, NULL},
  
  {"BOOLEAN", typecast_BOOLEAN_types, typecast_BOOLEAN_cast, NULL},
  {"DATETIME", typecast_DATETIME_types, typecast_DATETIME_cast, NULL},
  {"TIME", typecast_TIME_types, typecast_TIME_cast, NULL},
  {"DATE", typecast_DATE_types, typecast_DATE_cast, NULL},
  {"INTERVAL", typecast_INTERVAL_types, typecast_INTERVAL_cast, NULL},
  {"BINARY", typecast_BINARY_types, typecast_BINARY_cast, NULL},
  {"ROWID", typecast_ROWID_types, typecast_ROWID_cast, NULL},
  {"LONGINTEGERARRAY", typecast_LONGINTEGERARRAY_types, typecast_LONGINTEGERARRAY_cast, "LONGINTEGER"},
  {"INTEGERARRAY", typecast_INTEGERARRAY_types, typecast_INTEGERARRAY_cast, "INTEGER"},
  {"FLOATARRAY", typecast_FLOATARRAY_types, typecast_FLOATARRAY_cast, "FLOAT"},
  {"DECIMALARRAY", typecast_DECIMALARRAY_types, typecast_DECIMALARRAY_cast, "DECIMAL"},
  {"UNICODEARRAY", typecast_UNICODEARRAY_types, typecast_UNICODEARRAY_cast, "UNICODE"},
  {"STRINGARRAY", typecast_STRINGARRAY_types, typecast_STRINGARRAY_cast, "STRING"},
  {"BOOLEANARRAY", typecast_BOOLEANARRAY_types, typecast_BOOLEANARRAY_cast, "BOOLEAN"},
  {"DATETIMEARRAY", typecast_DATETIMEARRAY_types, typecast_DATETIMEARRAY_cast, "DATETIME"},
  {"TIMEARRAY", typecast_TIMEARRAY_types, typecast_TIMEARRAY_cast, "TIME"},
  {"DATEARRAY", typecast_DATEARRAY_types, typecast_DATEARRAY_cast, "DATE"},
  {"INTERVALARRAY", typecast_INTERVALARRAY_types, typecast_INTERVALARRAY_cast, "INTERVAL"},
  {"BINARYARRAY", typecast_BINARYARRAY_types, typecast_BINARYARRAY_cast, "BINARY"},
  {"ROWIDARRAY", typecast_ROWIDARRAY_types, typecast_ROWIDARRAY_cast, "ROWID"},
  {"UNKNOWN", typecast_UNKNOWN_types, typecast_UNKNOWN_cast, NULL},
  */
    {NULL, NULL, NULL, NULL}
};





/* eof */
