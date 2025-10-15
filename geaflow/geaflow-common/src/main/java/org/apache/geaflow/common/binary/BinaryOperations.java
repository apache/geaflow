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

package org.apache.geaflow.common.binary;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class BinaryOperations {

    private static final sun.misc.Unsafe UNSAFE;

    public static final int BOOLEAN_ARRAY_OFFSET;

    public static final int BYTE_ARRAY_OFFSET;

    public static final int SHORT_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET;

    public static final int DOUBLE_ARRAY_OFFSET;

    private static final int MAJOR_VERSION =
        Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);

    private static final boolean UNALIGNED;

    static {
        sun.misc.Unsafe unsafe;
        try {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (sun.misc.Unsafe) unsafeField.get(null);
        } catch (Throwable cause) {
            unsafe = null;
        }
        UNSAFE = unsafe;

        if (UNSAFE != null) {
            BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
            INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
            LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
            FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
            DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
        } else {
            BOOLEAN_ARRAY_OFFSET = 0;
            BYTE_ARRAY_OFFSET = 0;
            SHORT_ARRAY_OFFSET = 0;
            INT_ARRAY_OFFSET = 0;
            LONG_ARRAY_OFFSET = 0;
            FLOAT_ARRAY_OFFSET = 0;
            DOUBLE_ARRAY_OFFSET = 0;
        }
    }

    static {
        boolean _unaligned;
        String arch = System.getProperty("os.arch", "");
        if (arch.equals("ppc64le") || arch.equals("ppc64") || arch.equals("s390x")) {
            _unaligned = true;
        } else {
            try {
                Class<?> bitsClass =
                    Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
                if (UNSAFE != null && MAJOR_VERSION >= 9) {
                    // Java 9/10 and 11/12 have different field names.
                    Field unalignedField =
                        bitsClass.getDeclaredField(MAJOR_VERSION >= 11 ? "UNALIGNED" : "unaligned");
                    _unaligned = UNSAFE.getBoolean(
                        UNSAFE.staticFieldBase(unalignedField), UNSAFE.staticFieldOffset(unalignedField));
                } else {
                    Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
                    unalignedMethod.setAccessible(true);
                    _unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
                }
            } catch (Throwable t) {
                _unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
            }
        }
        UNALIGNED = _unaligned;
    }

    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    public static int getInt(IBinaryObject object, long offset) {
        return UNSAFE.getInt(object.getBaseObject(), object.getAbsoluteAddress(offset));
    }

    public static void putInt(IBinaryObject object, long offset, int value) {
        UNSAFE.putInt(object.getBaseObject(), object.getAbsoluteAddress(offset), value);
    }

    public static boolean getBoolean(IBinaryObject object, long offset) {
        return UNSAFE.getBoolean(object.getBaseObject(), object.getAbsoluteAddress(offset));
    }

    public static void putBoolean(IBinaryObject object, long offset, boolean value) {
        UNSAFE.putBoolean(object.getBaseObject(), object.getAbsoluteAddress(offset), value);
    }

    public static byte getByte(IBinaryObject object, long offset) {
        return UNSAFE.getByte(object.getBaseObject(), object.getAbsoluteAddress(offset));
    }

    public static void putByte(IBinaryObject object, long offset, byte value) {
        UNSAFE.putByte(object.getBaseObject(), object.getAbsoluteAddress(offset), value);
    }

    public static short getShort(IBinaryObject object, long offset) {
        return UNSAFE.getShort(object.getBaseObject(), object.getAbsoluteAddress(offset));
    }

    public static void putShort(IBinaryObject object, long offset, short value) {
        UNSAFE.putShort(object.getBaseObject(), object.getAbsoluteAddress(offset), value);
    }

    public static long getLong(IBinaryObject object, long offset) {
        return UNSAFE.getLong(object.getBaseObject(), object.getAbsoluteAddress(offset));
    }

    public static void putLong(IBinaryObject object, long offset, long value) {
        UNSAFE.putLong(object.getBaseObject(), object.getAbsoluteAddress(offset), value);
    }

    public static float getFloat(IBinaryObject object, long offset) {
        return UNSAFE.getFloat(object.getBaseObject(), object.getAbsoluteAddress(offset));
    }

    public static void putFloat(IBinaryObject object, long offset, float value) {
        UNSAFE.putFloat(object.getBaseObject(), object.getAbsoluteAddress(offset), value);
    }

    public static double getDouble(IBinaryObject object, long offset) {
        return UNSAFE.getDouble(object.getBaseObject(), object.getAbsoluteAddress(offset));
    }

    public static void putDouble(IBinaryObject object, long offset, double value) {
        UNSAFE.putDouble(object.getBaseObject(), object.getAbsoluteAddress(offset), value);
    }

    public static void copyMemory(byte[] src, long srcOffset, byte[] dst, long dstOffset, long length) {
        HeapBinaryObject srcObject = HeapBinaryObject.of(src);
        HeapBinaryObject dstObject = HeapBinaryObject.of(dst);
        copyMemory(srcObject, srcOffset, dstObject, dstOffset, length);
    }

    public static void copyMemory(byte[] src, long srcOffset, IBinaryObject dst, long dstOffset, long length) {
        HeapBinaryObject srcObject = HeapBinaryObject.of(src);
        copyMemory(srcObject, srcOffset, dst, dstOffset, length);
    }

    public static void copyMemory(IBinaryObject src, long srcOffset, byte[] dst, long dstOffset, long length) {
        HeapBinaryObject dstObject = HeapBinaryObject.of(dst);
        copyMemory(src, srcOffset, dstObject, dstOffset, length);
    }

    public static void copyMemory(IBinaryObject src, long srcOffset, IBinaryObject dst, long dstOffset, long length) {
        if (src.size() == 0 || length == 0) {
            return;
        }
        if (dst.getAbsoluteAddress(dstOffset) < src.getAbsoluteAddress(srcOffset)) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                UNSAFE.copyMemory(src.getBaseObject(), src.getAbsoluteAddress(srcOffset), dst.getBaseObject(),
                    dst.getAbsoluteAddress(dstOffset), size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                UNSAFE.copyMemory(src.getBaseObject(), src.getAbsoluteAddress(srcOffset),
                    dst.getBaseObject(), dst.getAbsoluteAddress(dstOffset), size);
                length -= size;
            }
        }
    }

    public static boolean arrayEquals(
        IBinaryObject leftBase, long leftOffset, IBinaryObject rightBase, long rightOffset, final long length) {
        int i = 0;

        // check if stars align and we can get both offsets to be aligned
        if ((leftOffset % 8) == (rightOffset % 8)) {
            while ((leftOffset + i) % 8 != 0 && i < length) {
                if (getByte(leftBase, leftOffset + i) != getByte(rightBase, rightOffset + i)) {
                    return false;
                }
                i += 1;
            }
        }
        if (UNALIGNED || (((leftOffset + i) % 8 == 0) && ((rightOffset + i) % 8 == 0))) {
            while (i <= length - 8) {
                if (getLong(leftBase, leftOffset + i) != getLong(rightBase, rightOffset + i)) {
                    return false;
                }
                i += 8;
            }
        }
        while (i < length) {
            if (getByte(leftBase, leftOffset + i) != getByte(rightBase, rightOffset + i)) {
                return false;
            }
            i += 1;
        }
        return true;
    }
}
