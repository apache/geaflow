/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.infer.exchange.serialize;

public class OpCodeConstant {

    public static final short MARK = '(';
    public static final short STOP = '.';
    public static final short POP = '0';
    public static final short POP_MARK = '1';
    public static final short DUP = '2';
    public static final short FLOAT = 'F';
    public static final short INT = 'I';
    public static final short BININT = 'J';
    public static final short BININT1 = 'K';
    public static final short LONG = 'L';
    public static final short BININT2 = 'M';
    public static final short NONE = 'N';
    public static final short PERSID = 'P';
    public static final short BINPERSID = 'Q';
    public static final short REDUCE = 'R';
    public static final short STRING = 'S';
    public static final short BINSTRING = 'T';
    public static final short SHORT_BINSTRING = 'U';
    public static final short UNICODE = 'V';
    public static final short BINUNICODE = 'X';
    public static final short APPEND = 'a';
    public static final short BUILD = 'b';
    public static final short GLOBAL = 'c';
    public static final short DICT = 'd';
    public static final short EMPTY_DICT = '}';
    public static final short APPENDS = 'e';
    public static final short GET = 'g';
    public static final short BINGET = 'h';
    public static final short INST = 'i';
    public static final short LONG_BINGET = 'j';
    public static final short LIST = 'l';
    public static final short EMPTY_LIST = ']';
    public static final short OBJ = 'o';
    public static final short PUT = 'p';
    public static final short BINPUT = 'q';
    public static final short LONG_BINPUT = 'r';
    public static final short SETITEM = 's';
    public static final short TUPLE = 't';
    public static final short EMPTY_TUPLE = ')';
    public static final short SETITEMS = 'u';
    public static final short BINFLOAT = 'G';
    public static final String TRUE = "I01\n";
    public static final String FALSE = "I00\n";
    public static final short PROTO = 0x80;
    public static final short NEWOBJ = 0x81;
    public static final short EXT1 = 0x82;
    public static final short EXT2 = 0x83;
    public static final short EXT4 = 0x84;
    public static final short TUPLE1 = 0x85;
    public static final short TUPLE2 = 0x86;
    public static final short TUPLE3 = 0x87;
    public static final short NEWTRUE = 0x88;
    public static final short NEWFALSE = 0x89;
    public static final short LONG1 = 0x8a;
    public static final short LONG4 = 0x8b;
    public static final short BINBYTES = 'B';
    public static final short SHORT_BINBYTES = 'C';
    public static final short SHORT_BINUNICODE = 0x8c;
    public static final short BINUNICODE8 = 0x8d;
    public static final short BINBYTES8 = 0x8e;
    public static final short EMPTY_SET = 0x8f;
    public static final short ADDITEMS = 0x90;
    public static final short FROZENSET = 0x91;
    public static final short MEMOIZE = 0x94;
    public static final short FRAME = 0x95;
    public static final short NEWOBJ_EX = 0x92;
    public static final short STACK_GLOBAL = 0x93;

    public static final short BYTEARRAY8 = 0x96;
    public static final short NEXT_BUFFER = 0x97;
    public static final short READONLY_BUFFER = 0x98;
}
