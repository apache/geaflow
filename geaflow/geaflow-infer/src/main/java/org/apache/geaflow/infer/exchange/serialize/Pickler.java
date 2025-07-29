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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Pickler {

    private static final String GET = "get";

    private static final String GET_CLASS = "getClass";

    private static final String IS = "is";

    private static final String CLASS_KEY = "__class__";

    private static final int PROTOCOL = 2;

    private static class Memo {

        public Object obj;
        public int index;

        public Memo(Object obj, int index) {
            this.obj = obj;
            this.index = index;
        }
    }

    private static final int MAX_RECURSE_DEPTH = 1000;

    private int recurse = 0;

    private OutputStream out;

    private static final Map<Class<?>, IObjectPickler> CUSTOM_PICKLER = new HashMap<>();

    private final boolean useMemo;

    private final boolean valueCompare;

    protected HashMap<Integer, Memo> memo;

    public Pickler() {
        this(true);
    }

    public Pickler(boolean useMemo) {
        this(useMemo, true);
    }

    public Pickler(boolean useMemo, boolean valueCompare) {
        this.useMemo = useMemo;
        this.valueCompare = valueCompare;
    }

    public void close() throws IOException {
        memo = null;
        if (out != null) {
            out.flush();
            out.close();
        }
    }

    public static synchronized void registerCustomPickler(Class<?> clazz, IObjectPickler pickler) {
        CUSTOM_PICKLER.put(clazz, pickler);
    }

    public byte[] dumps(Object o) throws PickleException, IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        dump(o, bo);
        bo.flush();
        return bo.toByteArray();
    }

    public void dump(Object o, OutputStream stream) throws IOException, PickleException {
        out = stream;
        recurse = 0;
        if (useMemo) {
            memo = new HashMap<>();
        }
        out.write(OpCodeConstant.PROTO);
        out.write(PROTOCOL);
        save(o);
        memo = null;
        out.write(OpCodeConstant.STOP);
        out.flush();
        if (recurse != 0) {
            throw new PickleException("recursive structure error, please report this problem");
        }
    }


    public void save(Object o) throws PickleException, IOException {
        recurse++;
        if (recurse > MAX_RECURSE_DEPTH) {
            throw new StackOverflowError("recursion too deep in Pickler.save (>" + MAX_RECURSE_DEPTH + ")");
        }

        if (o == null) {
            out.write(OpCodeConstant.NONE);
            recurse--;
            return;
        }

        Class<?> t = o.getClass();
        if (lookupMemo(t, o) || dispatch(t, o)) {
            recurse--;
            return;
        }

        throw new PickleException("couldn't pickle object of type " + t);
    }

    protected void writeMemo(Object obj) throws IOException {
        if (!this.useMemo) {
            return;
        }
        int hash = valueCompare ? obj.hashCode() : System.identityHashCode(obj);
        if (!memo.containsKey(hash)) {
            int memoIndex = memo.size();
            memo.put(hash, new Memo(obj, memoIndex));
            if (memoIndex <= 0xFF) {
                out.write(OpCodeConstant.BINPUT);
                out.write((byte) memoIndex);
            } else {
                out.write(OpCodeConstant.LONG_BINPUT);
                byte[] indexBytes = PickleUtils.integer2Bytes(memoIndex);
                out.write(indexBytes, 0, 4);
            }
        }
    }


    private boolean lookupMemo(Class<?> objectType, Object obj) throws IOException {
        if (!this.useMemo) {
            return false;
        }
        if (!objectType.isPrimitive()) {
            int hash = valueCompare ? obj.hashCode() : System.identityHashCode(obj);
            if (memo.containsKey(hash)
                && (valueCompare ? memo.get(hash).obj.equals(obj) : memo.get(hash).obj == obj)) {
                int memoIndex = memo.get(hash).index;
                if (memoIndex <= 0xff) {
                    out.write(OpCodeConstant.BINGET);
                    out.write((byte) memoIndex);
                } else {
                    out.write(OpCodeConstant.LONG_BINGET);
                    byte[] indexBytes = PickleUtils.integer2Bytes(memoIndex);
                    out.write(indexBytes, 0, 4);
                }
                return true;
            }
        }
        return false;
    }

    private boolean dispatch(Class<?> t, Object o) throws IOException {
        Class<?> componentType = t.getComponentType();
        if (componentType != null) {
            if (componentType.isPrimitive()) {
                putArrayOfPrimitives(componentType, o);
            } else {
                putArrayOfObjects((Object[]) o);
            }
            return true;
        }

        if (o instanceof Boolean || t.equals(Boolean.TYPE)) {
            putBool((Boolean) o);
            return true;
        }
        if (o instanceof Byte || t.equals(Byte.TYPE)) {
            putLong(((Byte) o).longValue());
            return true;
        }
        if (o instanceof Short || t.equals(Short.TYPE)) {
            putLong(((Short) o).longValue());
            return true;
        }
        if (o instanceof Integer || t.equals(Integer.TYPE)) {
            putLong(((Integer) o).longValue());
            return true;
        }
        if (o instanceof Long || t.equals(Long.TYPE)) {
            putLong(((Long) o).longValue());
            return true;
        }
        if (o instanceof Float || t.equals(Float.TYPE)) {
            putFloat(((Float) o).doubleValue());
            return true;
        }
        if (o instanceof Double || t.equals(Double.TYPE)) {
            putFloat(((Double) o).doubleValue());
            return true;
        }
        if (o instanceof Character || t.equals(Character.TYPE)) {
            putString("" + o);
            return true;
        }

        IObjectPickler customPickler = getCustomPickler(t);
        if (customPickler != null) {
            customPickler.pickle(o, this.out, this);
            writeMemo(o);
            return true;
        }

        if (o instanceof String) {
            putString((String) o);
            return true;
        }
        if (o instanceof BigInteger) {
            putBigint((BigInteger) o);
            return true;
        }
        if (o instanceof BigDecimal) {
            putDecimal((BigDecimal) o);
            return true;
        }
        if (o instanceof Enum) {
            putString(o.toString());
            return true;
        }
        if (o instanceof Set<?>) {
            putSet((Set<?>) o);
            return true;
        }
        if (o instanceof Map<?, ?>) {
            putMap((Map<?, ?>) o);
            return true;
        }
        if (o instanceof List<?>) {
            putCollection((List<?>) o);
            return true;
        }
        if (o instanceof Collection<?>) {
            putCollection((Collection<?>) o);
            return true;
        }
        if (o instanceof java.io.Serializable) {
            putJavabean(o);
            return true;
        }
        return false;
    }

    private synchronized IObjectPickler getCustomPickler(Class<?> t) {
        IObjectPickler pickler = CUSTOM_PICKLER.get(t);
        if (pickler != null) {
            return pickler;
        }
        for (Entry<Class<?>, IObjectPickler> x : CUSTOM_PICKLER.entrySet()) {
            if (x.getKey().isAssignableFrom(t)) {
                return x.getValue();
            }
        }
        return null;
    }

    private void putCollection(Collection<?> list) throws IOException {
        out.write(OpCodeConstant.EMPTY_LIST);
        writeMemo(list);
        out.write(OpCodeConstant.MARK);
        for (Object o : list) {
            save(o);
        }
        out.write(OpCodeConstant.APPENDS);
    }

    private void putMap(Map<?, ?> o) throws IOException {
        out.write(OpCodeConstant.EMPTY_DICT);
        writeMemo(o);
        out.write(OpCodeConstant.MARK);
        for (Object k : o.keySet()) {
            save(k);
            save(o.get(k));
        }
        out.write(OpCodeConstant.SETITEMS);
    }

    private void putSet(Set<?> o) throws IOException {
        out.write(OpCodeConstant.GLOBAL);
        out.write("__builtin__\nset\n".getBytes());
        out.write(OpCodeConstant.EMPTY_LIST);
        out.write(OpCodeConstant.MARK);
        for (Object x : o) {
            save(x);
        }
        out.write(OpCodeConstant.APPENDS);
        out.write(OpCodeConstant.TUPLE1);
        out.write(OpCodeConstant.REDUCE);
        writeMemo(o);
    }

    private void putArrayOfObjects(Object[] array) throws IOException {
        if (array.length == 0) {
            out.write(OpCodeConstant.EMPTY_TUPLE);
        } else if (array.length == 1) {
            if (array[0] == array) {
                throw new PickleException("recursive array not supported, use list");
            }
            save(array[0]);
            out.write(OpCodeConstant.TUPLE1);
        } else if (array.length == 2) {
            if (array[0] == array || array[1] == array) {
                throw new PickleException("recursive array not supported, use list");
            }
            save(array[0]);
            save(array[1]);
            out.write(OpCodeConstant.TUPLE2);
        } else if (array.length == 3) {
            if (array[0] == array || array[1] == array || array[2] == array) {
                throw new PickleException("recursive array not supported, use list");
            }
            save(array[0]);
            save(array[1]);
            save(array[2]);
            out.write(OpCodeConstant.TUPLE3);
        } else {
            out.write(OpCodeConstant.MARK);
            for (Object o : array) {
                if (o == array) {
                    throw new PickleException("recursive array not supported, use list");
                }
                save(o);
            }
            out.write(OpCodeConstant.TUPLE);
        }
        writeMemo(array);
    }

    private void putArrayOfPrimitives(Class<?> t, Object array) throws IOException {
        if (t.equals(Boolean.TYPE)) {
            boolean[] source = (boolean[]) array;
            Boolean[] boolArray = new Boolean[source.length];
            for (int i = 0; i < source.length; ++i) {
                boolArray[i] = source[i];
            }
            putArrayOfObjects(boolArray);
            return;
        }
        if (t.equals(Character.TYPE)) {
            String s = new String((char[]) array);
            putString(s);
            return;
        }
        if (t.equals(Byte.TYPE)) {
            out.write(OpCodeConstant.GLOBAL);
            out.write("__builtin__\nbytearray\n".getBytes());
            String str = PickleUtils.rawStringFromBytes((byte[]) array);
            putString(str);
            putString("latin-1");
            out.write(OpCodeConstant.TUPLE2);
            out.write(OpCodeConstant.REDUCE);
            writeMemo(array);
            return;
        }

        out.write(OpCodeConstant.GLOBAL);
        out.write("array\narray\n".getBytes());
        out.write(OpCodeConstant.SHORT_BINSTRING);
        out.write(1);

        if (t.equals(Short.TYPE)) {
            out.write('h');
            out.write(OpCodeConstant.EMPTY_LIST);
            out.write(OpCodeConstant.MARK);
            for (short s : (short[]) array) {
                save(s);
            }
        } else if (t.equals(Integer.TYPE)) {
            out.write('i');
            out.write(OpCodeConstant.EMPTY_LIST);
            out.write(OpCodeConstant.MARK);
            for (int i : (int[]) array) {
                save(i);
            }
        } else if (t.equals(Long.TYPE)) {
            out.write('l');
            out.write(OpCodeConstant.EMPTY_LIST);
            out.write(OpCodeConstant.MARK);
            for (long v : (long[]) array) {
                save(v);
            }
        } else if (t.equals(Float.TYPE)) {
            out.write('f');
            out.write(OpCodeConstant.EMPTY_LIST);
            out.write(OpCodeConstant.MARK);
            for (float f : (float[]) array) {
                save(f);
            }
        } else if (t.equals(Double.TYPE)) {
            out.write('d');
            out.write(OpCodeConstant.EMPTY_LIST);
            out.write(OpCodeConstant.MARK);
            for (double d : (double[]) array) {
                save(d);
            }
        }

        out.write(OpCodeConstant.APPENDS);
        out.write(OpCodeConstant.TUPLE2);
        out.write(OpCodeConstant.REDUCE);

        writeMemo(array);
    }

    private void putDecimal(BigDecimal d) throws IOException {
        out.write(OpCodeConstant.GLOBAL);
        out.write("decimal\nDecimal\n".getBytes());
        putString(d.toEngineeringString());
        out.write(OpCodeConstant.TUPLE1);
        out.write(OpCodeConstant.REDUCE);
        writeMemo(d);
    }


    private void putBigint(BigInteger i) throws IOException {
        byte[] b = PickleUtils.encodeLong(i);
        if (b.length <= 0xff) {
            out.write(OpCodeConstant.LONG1);
            out.write(b.length);
            out.write(b);
        } else {
            out.write(OpCodeConstant.LONG4);
            out.write(PickleUtils.integer2Bytes(b.length));
            out.write(b);
        }
        writeMemo(i);
    }

    private void putString(String string) throws IOException {
        byte[] encoded = string.getBytes(StandardCharsets.UTF_8);
        out.write(OpCodeConstant.BINUNICODE);
        out.write(PickleUtils.integer2Bytes(encoded.length));
        out.write(encoded);
        writeMemo(string);
    }

    private void putFloat(double d) throws IOException {
        out.write(OpCodeConstant.BINFLOAT);
        out.write(PickleUtils.double2Bytes(d));
    }

    private void putLong(long v) throws IOException {
        if (v >= 0) {
            if (v <= 0xff) {
                out.write(OpCodeConstant.BININT1);
                out.write((int) v);
                return;
            }
            if (v <= 0xffff) {
                out.write(OpCodeConstant.BININT2);
                out.write((int) v & 0xff);
                out.write((int) v >> 8);
                return;
            }
        }

        long highBits = v >> 31;
        if (highBits == 0 || highBits == -1) {
            out.write(OpCodeConstant.BININT);
            out.write(PickleUtils.integer2Bytes((int) v));
            return;
        }
        putBigint(BigInteger.valueOf(v));
    }

    private void putBool(boolean b) throws IOException {
        if (b) {
            out.write(OpCodeConstant.NEWTRUE);
        } else {
            out.write(OpCodeConstant.NEWFALSE);
        }
    }

    private void putJavabean(Object o) throws PickleException, IOException {
        Map<String, Object> map = new HashMap<>();
        try {
            for (Method m : o.getClass().getMethods()) {
                int modifiers = m.getModifiers();
                if ((modifiers & Modifier.PUBLIC) != 0 && (modifiers & Modifier.STATIC) == 0) {
                    String methodName = m.getName();
                    int prefixLen;
                    if (methodName.equals(GET_CLASS)) {
                        continue;
                    }
                    if (methodName.startsWith(GET)) {
                        prefixLen = 3;
                    } else if (methodName.startsWith(IS)) {
                        prefixLen = 2;
                    } else {
                        continue;
                    }
                    Object value = m.invoke(o);
                    String name = methodName.substring(prefixLen);
                    if (name.length() == 1) {
                        name = name.toLowerCase();
                    } else {
                        if (!Character.isUpperCase(name.charAt(1))) {
                            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
                        }
                    }
                    map.put(name, value);
                }
            }
            map.put(CLASS_KEY, o.getClass().getName());
            save(map);
        } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
            throw new PickleException("couldn't introspect javabean: " + e);
        }
    }
}
