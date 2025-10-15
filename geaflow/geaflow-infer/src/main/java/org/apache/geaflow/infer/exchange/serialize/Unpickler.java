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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Unpickler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Unpickler.class);

    private static final String SET_STATE = "setState";

    private static final String PYTHON_EXCEPTION = "python_exception";

    private static final String MODULE_SUFFIX = ".";

    private static final String LONG_END_WITH = "L";

    private static final String EXCEPTIONS = "exceptions";

    private static final String ERROR = "Error";

    private static final String EXCEPTION = "Exception";

    private static final String WARNING = "Warning";

    private static final String SYSTEM_EXIT = "SystemExit";

    private static final String GENERATOR_EXIT = "GeneratorExit";

    private static final String KEYBOARD_INTERRUPT = "KeyboardInterrupt";

    private static final String STOP_ITERATION = "StopIteration";

    private static final String BUILTINS = "builtins";

    private static final String BUILTIN = "__builtin__";

    protected static final Object NO_RETURN_VALUE = new Object();

    protected static final int HIGHEST_PROTOCOL = 5;

    protected Map<Integer, Object> memo;

    protected UnpickleStack stack;

    protected InputStream input;

    protected static Map<String, IObjectConstructor> objectConstructors;

    static {
        objectConstructors = new HashMap<>();
        objectConstructors.put("__builtin__.complex", new ComplexNumberConstructor());
        objectConstructors.put("builtins.complex", new ComplexNumberConstructor());
        objectConstructors.put("array.array", new ArrayConstructor());
        objectConstructors.put("array._array_reconstructor", new ArrayConstructor());
        objectConstructors.put("__builtin__.bytearray", new ByteArrayConstructor());
        objectConstructors.put("builtins.bytearray", new ByteArrayConstructor());
        objectConstructors.put("__builtin__.bytes", new ByteArrayConstructor());
        objectConstructors.put("_codecs.encode", new ByteArrayConstructor());
    }

    public Unpickler() {
        memo = new HashMap<>();
        stack = new UnpickleStack();
    }

    public static void registerConstructor(String module, String classname, IObjectConstructor constructor) {
        objectConstructors.put(module + MODULE_SUFFIX + classname, constructor);
    }

    public Object load(InputStream stream) throws PickleException, IOException {
        input = stream;
        while (true) {
            short key = PickleUtils.readByte(input);
            if (key == -1) {
                throw new IOException("premature end of file");
            }
            Object value = dispatch(key);
            if (value != NO_RETURN_VALUE) {
                return value;
            }
        }
    }


    public Object loads(byte[] pickleData) throws Exception {
        Object loadResult = load(new ByteArrayInputStream(pickleData));
        if (loadResult instanceof String) {
            if (loadResult.toString().startsWith(PYTHON_EXCEPTION)) {
                throw new RuntimeException(loadResult.toString());
            }
        }
        return loadResult;
    }

    public void close() {
        if (stack != null) {
            stack.clear();
        }
        if (memo != null) {
            memo.clear();
        }
        if (input != null) {
            try {
                input.close();
            } catch (IOException e) {
                LOGGER.error("input closed fail");
            }
        }
    }

    protected Object nextBuffer() throws PickleException {
        throw new PickleException("pickle stream refers to out-of-band data "
            + "but no user-overridden nextBuffer() method is used");
    }

    protected Object dispatch(short key) throws PickleException, IOException {
        switch (key) {
            case OpCodeConstant.MARK:
                loadMark();
                break;
            case OpCodeConstant.STOP:
                Object value = stack.pop();
                stack.clear();
                memo.clear();
                return value;
            case OpCodeConstant.POP:
                loadPop();
                break;
            case OpCodeConstant.POP_MARK:
                loadPopMark();
                break;
            case OpCodeConstant.DUP:
                loadDup();
                break;
            case OpCodeConstant.FLOAT:
                loadFloat();
                break;
            case OpCodeConstant.INT:
                loadInt();
                break;
            case OpCodeConstant.BININT:
                loadBinint();
                break;
            case OpCodeConstant.BININT1:
                loadBinint1();
                break;
            case OpCodeConstant.LONG:
                loadLong();
                break;
            case OpCodeConstant.BININT2:
                loadBinint2();
                break;
            case OpCodeConstant.NONE:
                loadNone();
                break;
            case OpCodeConstant.PERSID:
                loadPersid();
                break;
            case OpCodeConstant.BINPERSID:
                loadBinpersid();
                break;
            case OpCodeConstant.REDUCE:
                loadReduce();
                break;
            case OpCodeConstant.STRING:
                loadString();
                break;
            case OpCodeConstant.BINSTRING:
                loadBinstring();
                break;
            case OpCodeConstant.SHORT_BINSTRING:
                loadShortBinstring();
                break;
            case OpCodeConstant.UNICODE:
                loadUnicode();
                break;
            case OpCodeConstant.BINUNICODE:
                loadBinunicode();
                break;
            case OpCodeConstant.APPEND:
                loadAppend();
                break;
            case OpCodeConstant.BUILD:
                loadBuild();
                break;
            case OpCodeConstant.GLOBAL:
                loadGlobal();
                break;
            case OpCodeConstant.DICT:
                loadDict();
                break;
            case OpCodeConstant.EMPTY_DICT:
                loadEmptyDictionary();
                break;
            case OpCodeConstant.APPENDS:
                loadAppends();
                break;
            case OpCodeConstant.GET:
                loadGet();
                break;
            case OpCodeConstant.BINGET:
                loadBinget();
                break;
            case OpCodeConstant.INST:
                loadInst();
                break;
            case OpCodeConstant.LONG_BINGET:
                loadLongBinget();
                break;
            case OpCodeConstant.LIST:
                loadList();
                break;
            case OpCodeConstant.EMPTY_LIST:
                loadEmptyList();
                break;
            case OpCodeConstant.OBJ:
                loadObj();
                break;
            case OpCodeConstant.PUT:
                loadPut();
                break;
            case OpCodeConstant.BINPUT:
                loadBinput();
                break;
            case OpCodeConstant.LONG_BINPUT:
                loadLongBinput();
                break;
            case OpCodeConstant.SETITEM:
                loadSetitem();
                break;
            case OpCodeConstant.TUPLE:
                loadTuple();
                break;
            case OpCodeConstant.EMPTY_TUPLE:
                loadEmptyTuple();
                break;
            case OpCodeConstant.SETITEMS:
                loadSetitems();
                break;
            case OpCodeConstant.BINFLOAT:
                loadBinfloat();
                break;

            case OpCodeConstant.PROTO:
                loadProto();
                break;
            case OpCodeConstant.NEWOBJ:
                loadNewBbj();
                break;
            case OpCodeConstant.EXT1:
            case OpCodeConstant.EXT2:
            case OpCodeConstant.EXT4:
                throw new PickleException("Unimplemented opcode EXT1/EXT2/EXT4 encountered.");
            case OpCodeConstant.TUPLE1:
                loadTuple1();
                break;
            case OpCodeConstant.TUPLE2:
                loadTuple2();
                break;
            case OpCodeConstant.TUPLE3:
                loadTuple3();
                break;
            case OpCodeConstant.NEWTRUE:
                loadTrue();
                break;
            case OpCodeConstant.NEWFALSE:
                loadFalse();
                break;
            case OpCodeConstant.LONG1:
                loadLong1();
                break;
            case OpCodeConstant.LONG4:
                loadLong4();
                break;

            case OpCodeConstant.BINBYTES:
                loadBinBytes();
                break;
            case OpCodeConstant.SHORT_BINBYTES:
                loadShortBinBytes();
                break;

            case OpCodeConstant.BINUNICODE8:
                loadBinunicode8();
                break;
            case OpCodeConstant.SHORT_BINUNICODE:
                loadShortBinunicode();
                break;
            case OpCodeConstant.BINBYTES8:
                loadBinBytes8();
                break;
            case OpCodeConstant.EMPTY_SET:
                loadEmptySet();
                break;
            case OpCodeConstant.ADDITEMS:
                loadAddItems();
                break;
            case OpCodeConstant.FROZENSET:
                loadFrozenset();
                break;
            case OpCodeConstant.MEMOIZE:
                loadMemoize();
                break;
            case OpCodeConstant.FRAME:
                loadFrame();
                break;
            case OpCodeConstant.NEWOBJ_EX:
                loadNewObjEx();
                break;
            case OpCodeConstant.STACK_GLOBAL:
                loadStackGlobal();
                break;

            case OpCodeConstant.BYTEARRAY8:
                loadByteArray8();
                break;
            case OpCodeConstant.READONLY_BUFFER:
                loadReadonlyBuffer();
                break;
            case OpCodeConstant.NEXT_BUFFER:
                loadNextBuffer();
                break;

            default:
                throw new InvalidOpcodeException("invalid pickle opcode: " + key);
        }

        return NO_RETURN_VALUE;
    }

    private void loadReadonlyBuffer() {
    }

    private void loadNextBuffer() throws PickleException {
        stack.add(nextBuffer());
    }

    private void loadByteArray8() throws IOException {
        long len = PickleUtils.bytes2Long(PickleUtils.readBytes(input, 8), 0);
        stack.add(PickleUtils.readBytes(input, len));
    }

    private void loadBuild() {
        Object args = stack.pop();
        Object target = stack.peek();
        try {
            Method setStateMethod = target.getClass().getMethod(SET_STATE, args.getClass());
            setStateMethod.invoke(target, args);
        } catch (Exception e) {
            throw new PickleException("failed to setState()", e);
        }
    }

    private void loadProto() throws IOException {
        short proto = PickleUtils.readByte(input);
        if (proto < 0 || proto > HIGHEST_PROTOCOL) {
            throw new PickleException("unsupported pickle protocol: " + proto);
        }
    }

    private void loadNone() {
        stack.add(null);
    }

    private void loadFalse() {
        stack.add(false);
    }

    private void loadTrue() {
        stack.add(true);
    }

    private void loadInt() throws IOException {
        String data = PickleUtils.readLine(input, true);
        Object val;
        if (data.equals(OpCodeConstant.FALSE.substring(1))) {
            val = false;
        } else if (data.equals(OpCodeConstant.TRUE.substring(1))) {
            val = true;
        } else {
            String number = data.substring(0, data.length() - 1);
            try {
                val = Integer.parseInt(number, 10);
            } catch (NumberFormatException x) {
                val = Long.parseLong(number, 10);
            }
        }
        stack.add(val);
    }

    private void loadBinint() throws IOException {
        int integer = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 4));
        stack.add(integer);
    }

    private void loadBinint1() throws IOException {
        stack.add((int) PickleUtils.readByte(input));
    }

    private void loadBinint2() throws IOException {
        int integer = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 2));
        stack.add(integer);
    }

    private void loadLong() throws IOException {
        String val = PickleUtils.readLine(input);
        if (val.endsWith(LONG_END_WITH)) {
            val = val.substring(0, val.length() - 1);
        }
        BigInteger bi = new BigInteger(val);
        stack.add(PickleUtils.optimizeBigint(bi));
    }

    private void loadLong1() throws IOException {
        short n = PickleUtils.readByte(input);
        byte[] data = PickleUtils.readBytes(input, n);
        stack.add(PickleUtils.decodeLong(data));
    }

    private void loadLong4() throws IOException {
        int n = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 4));
        byte[] data = PickleUtils.readBytes(input, n);
        stack.add(PickleUtils.decodeLong(data));
    }

    private void loadFloat() throws IOException {
        String val = PickleUtils.readLine(input, true);
        stack.add(Double.parseDouble(val));
    }

    private void loadBinfloat() throws IOException {
        double val = PickleUtils.bytes2Double(PickleUtils.readBytes(input, 8), 0);
        stack.add(val);
    }

    private void loadString() throws IOException {
        String rep = PickleUtils.readLine(input);
        boolean quotesOk = false;
        for (String q : new String[]{"\"", "'"}) {
            if (rep.startsWith(q)) {
                if (!rep.endsWith(q)) {
                    throw new PickleException("insecure string pickle");
                }
                rep = rep.substring(1, rep.length() - 1);
                quotesOk = true;
                break;
            }
        }

        if (!quotesOk) {
            throw new PickleException("insecure string pickle");
        }

        stack.add(PickleUtils.decodeEscaped(rep));
    }

    private void loadBinstring() throws IOException {
        int len = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 4));
        byte[] data = PickleUtils.readBytes(input, len);
        stack.add(PickleUtils.rawStringFromBytes(data));
    }

    private void loadBinBytes() throws IOException {
        int len = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 4));
        stack.add(PickleUtils.readBytes(input, len));
    }

    private void loadBinBytes8() throws IOException {
        long len = PickleUtils.bytes2Long(PickleUtils.readBytes(input, 8), 0);
        stack.add(PickleUtils.readBytes(input, len));
    }

    private void loadUnicode() throws IOException {
        String str = PickleUtils.decodeUnicodeEscaped(PickleUtils.readLine(input));
        stack.add(str);
    }

    private void loadBinunicode() throws IOException {
        int len = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 4));
        byte[] data = PickleUtils.readBytes(input, len);
        stack.add(new String(data, StandardCharsets.UTF_8));
    }

    private void loadBinunicode8() throws IOException {
        long len = PickleUtils.bytes2Long(PickleUtils.readBytes(input, 8), 0);
        byte[] data = PickleUtils.readBytes(input, len);
        stack.add(new String(data, StandardCharsets.UTF_8));
    }

    private void loadShortBinunicode() throws IOException {
        int len = PickleUtils.readByte(input);
        byte[] data = PickleUtils.readBytes(input, len);
        stack.add(new String(data, StandardCharsets.UTF_8));
    }

    private void loadShortBinstring() throws IOException {
        short len = PickleUtils.readByte(input);
        byte[] data = PickleUtils.readBytes(input, len);
        stack.add(PickleUtils.rawStringFromBytes(data));
    }

    private void loadShortBinBytes() throws IOException {
        short len = PickleUtils.readByte(input);
        stack.add(PickleUtils.readBytes(input, len));
    }

    private void loadTuple() {
        List<Object> top = stack.popAllSinceMarker();
        stack.add(top.toArray());
    }

    private void loadEmptyTuple() {
        stack.add(new Object[0]);
    }

    private void loadTuple1() {
        stack.add(new Object[]{stack.pop()});
    }

    private void loadTuple2() {
        Object o2 = stack.pop();
        Object o1 = stack.pop();
        stack.add(new Object[]{o1, o2});
    }

    private void loadTuple3() {
        Object o3 = stack.pop();
        Object o2 = stack.pop();
        Object o1 = stack.pop();
        stack.add(new Object[]{o1, o2, o3});
    }

    private void loadEmptyList() {
        stack.add(new ArrayList<>(0));
    }

    private void loadEmptyDictionary() {
        stack.add(new HashMap<>(0));
    }

    private void loadEmptySet() {
        stack.add(new HashSet<>());
    }

    private void loadList() {
        List<Object> top = stack.popAllSinceMarker();
        stack.add(top);
    }

    private void loadDict() {
        List<Object> top = stack.popAllSinceMarker();
        HashMap<Object, Object> map = new HashMap<>(top.size());
        for (int i = 0; i < top.size(); i += 2) {
            Object key = top.get(i);
            Object value = top.get(i + 1);
            map.put(key, value);
        }
        stack.add(map);
    }

    private void loadFrozenset() {
        List<Object> top = stack.popAllSinceMarker();
        HashSet<Object> set = new HashSet<>();
        set.addAll(top);
        stack.add(set);
    }

    private void loadAddItems() {
        List<Object> top = stack.popAllSinceMarker();
        HashSet<Object> set = (HashSet<Object>) stack.pop();
        set.addAll(top);
        stack.add(set);
    }

    private void loadGlobal() throws IOException {
        String module = PickleUtils.readLine(input);
        String name = PickleUtils.readLine(input);
        loadGlobalSub(module, name);
    }

    private void loadStackGlobal() {
        String name = (String) stack.pop();
        String module = (String) stack.pop();
        loadGlobalSub(module, name);
    }

    private void loadGlobalSub(String module, String name) {
        IObjectConstructor constructor = objectConstructors.get(module + MODULE_SUFFIX + name);
        if (constructor == null) {
            if (module.equals(EXCEPTIONS)) {
                constructor = new ExceptionConstructor(PythonException.class, module, name);
            } else if (module.equals(BUILTIN) || module.equals(BUILTINS)) {
                if (name.endsWith(ERROR) || name.endsWith(WARNING)
                    || name.endsWith(EXCEPTION) || name.equals(KEYBOARD_INTERRUPT)
                    || name.equals(STOP_ITERATION) || name.equals(GENERATOR_EXIT)
                    || name.equals(SYSTEM_EXIT)) {
                    constructor = new ExceptionConstructor(PythonException.class, module, name);
                } else {
                    constructor = new DictionaryConstructor(module, name);
                }
            } else {
                constructor = new DictionaryConstructor(module, name);
            }
        }
        stack.add(constructor);
    }

    private void loadPop() {
        stack.pop();
    }

    private void loadPopMark() {
        Object o;
        do {
            o = stack.pop();
        } while (o != stack.marker);
        stack.trim();
    }

    private void loadDup() {
        stack.add(stack.peek());
    }

    private void loadGet() throws IOException {
        int i = Integer.parseInt(PickleUtils.readLine(input), 10);
        if (!memo.containsKey(i)) {
            throw new PickleException("invalid memo key");
        }
        stack.add(memo.get(i));
    }

    private void loadBinget() throws IOException {
        int i = PickleUtils.readByte(input);
        if (!memo.containsKey(i)) {
            throw new PickleException("invalid memo key");
        }
        stack.add(memo.get(i));
    }

    private void loadLongBinget() throws IOException {
        int i = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 4));
        if (!memo.containsKey(i)) {
            throw new PickleException("invalid memo key");
        }
        stack.add(memo.get(i));
    }

    private void loadPut() throws IOException {
        int i = Integer.parseInt(PickleUtils.readLine(input), 10);
        memo.put(i, stack.peek());
    }

    private void loadBinput() throws IOException {
        int i = PickleUtils.readByte(input);
        memo.put(i, stack.peek());
    }

    private void loadLongBinput() throws IOException {
        int i = PickleUtils.bytes2Integer(PickleUtils.readBytes(input, 4));
        memo.put(i, stack.peek());
    }

    private void loadMemoize() {
        memo.put(memo.size(), stack.peek());
    }

    private void loadAppend() {
        Object value = stack.pop();
        ArrayList<Object> list = (ArrayList<Object>) stack.peek();
        list.add(value);
    }

    private void loadAppends() {
        List<Object> top = stack.popAllSinceMarker();
        ArrayList<Object> list = (ArrayList<Object>) stack.peek();
        list.addAll(top);
        list.trimToSize();
    }

    private void loadSetitem() {
        Object value = stack.pop();
        Object key = stack.pop();
        Map<Object, Object> dict = (Map<Object, Object>) stack.peek();
        dict.put(key, value);
    }

    private void loadSetitems() {
        HashMap<Object, Object> newItems = new HashMap<>();
        Object value = stack.pop();
        while (value != stack.marker) {
            Object key = stack.pop();
            newItems.put(key, value);
            value = stack.pop();
        }

        Map<Object, Object> dict = (Map<Object, Object>) stack.peek();
        dict.putAll(newItems);
    }

    private void loadMark() {
        stack.addMark();
    }

    private void loadReduce() {
        Object[] args = (Object[]) stack.pop();
        IObjectConstructor constructor = (IObjectConstructor) stack.pop();
        stack.add(constructor.construct(args));
    }

    private void loadNewBbj() {
        loadReduce();
    }

    private void loadNewObjEx() {
        HashMap<?, ?> kwargs = (HashMap<?, ?>) stack.pop();
        Object[] args = (Object[]) stack.pop();
        IObjectConstructor constructor = (IObjectConstructor) stack.pop();
        if (kwargs.size() == 0) {
            stack.add(constructor.construct(args));
        } else {
            throw new PickleException("loadNewObjEx with keyword arguments not supported");
        }
    }

    private void loadFrame() throws IOException {
        PickleUtils.readBytes(input, 8);
    }

    private void loadPersid() throws IOException {
        String pid = PickleUtils.readLine(input);
        stack.add(persistentLoad(pid));
    }

    private void loadBinpersid() {
        String pid = stack.pop().toString();
        stack.add(persistentLoad(pid));
    }

    private void loadObj() {
        List<Object> args = stack.popAllSinceMarker();
        IObjectConstructor constructor = (IObjectConstructor) args.get(0);
        args = args.subList(1, args.size());
        Object object = constructor.construct(args.toArray());
        stack.add(object);
    }

    private void loadInst() throws IOException {
        String module = PickleUtils.readLine(input);
        String classname = PickleUtils.readLine(input);
        List<Object> args = stack.popAllSinceMarker();
        IObjectConstructor constructor = objectConstructors.get(module + MODULE_SUFFIX + classname);
        if (constructor == null) {
            constructor = new DictionaryConstructor(module, classname);
            args.clear();
        }
        Object object = constructor.construct(args.toArray());
        stack.add(object);
    }

    protected Object persistentLoad(String pid) {
        throw new PickleException("A load persistent id instruction was encountered, "
            + "but no persistentLoad function was specified. pid: " + pid);
    }
}
