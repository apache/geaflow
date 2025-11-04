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

package org.apache.geaflow.memory;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.geaflow.common.utils.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This file is based on source code from the Netty Project (http://netty.io/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Netty's io.netty.util.internal.PlatformDependent.
 */
public final class PlatformDependent implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlatformDependent.class);

    static final sun.misc.Unsafe UNSAFE;
    // constants borrowed from murmur3
    static final int HASH_CODE_ASCII_SEED = 0xc2b2ae35;
    static final int HASH_CODE_C1 = 0xcc9e2d51;
    static final int HASH_CODE_C2 = 0x1b873593;
    private static final long ADDRESS_FIELD_OFFSET;
    private static final long BYTE_ARRAY_BASE_OFFSET;
    private static final Constructor<?> DIRECT_BUFFER_CONSTRUCTOR;
    private static final Throwable UNSAFE_UNAVAILABILITY_CAUSE;
    /**
     * Limits the number of bytes to copy per {@link sun.misc.Unsafe#copyMemory(long, long, long)} to allow
     * safepoint polling
     * during a large copy.
     */
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    static {
        final ByteBuffer direct;
        Field addressField = null;
        Throwable unsafeUnavailabilityCause = null;
        sun.misc.Unsafe unsafe;

        direct = ByteBuffer.allocateDirect(1);

        // attempt to access field Unsafe#theUnsafe
        final Object maybeUnsafe = AccessController
            .doPrivileged((PrivilegedAction<Object>) () -> {
                try {
                    final Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                    // We always want to try using Unsafe as the access still works on java9
                    // as well and
                    // we need it for out native-transports and many optimizations.
                    Throwable cause = ReflectionUtil.trySetAccessible(unsafeField, false);
                    if (cause != null) {
                        return cause;
                    }
                    // the unsafe instance
                    return unsafeField.get(null);
                } catch (NoSuchFieldException | SecurityException | IllegalAccessException | NoClassDefFoundError e) {
                    return e;
                } // Also catch NoClassDefFoundError in case someone uses for example OSGI
                // and it made
                // Unsafe unloadable.
            });

        // the conditional check here can not be replaced with checking that maybeUnsafe
        // is an instanceof Unsafe and reversing the if and else blocks; this is because an
        // instanceof check against Unsafe will trigger a class load and we might not have
        // the runtime permission accessClassInPackage.sun.misc
        if (maybeUnsafe instanceof Throwable) {
            unsafe = null;
            unsafeUnavailabilityCause = (Throwable) maybeUnsafe;
        } else {
            unsafe = (sun.misc.Unsafe) maybeUnsafe;
        }

        // ensure the unsafe supports all necessary methods to work around the mistake in the
        // latest OpenJDK
        // https://github.com/netty/netty/issues/1061
        // http://www.mail-archive.com/jdk6-dev@openjdk.java.net/msg00698.html
        if (unsafe != null) {
            final sun.misc.Unsafe finalUnsafe = unsafe;
            final Object maybeException = AccessController
                .doPrivileged((PrivilegedAction<Object>) () -> {
                    try {
                        finalUnsafe.getClass()
                            .getDeclaredMethod("copyMemory", Object.class, long.class,
                                Object.class, long.class, long.class);
                        return null;
                    } catch (NoSuchMethodException | SecurityException e) {
                        return e;
                    }
                });

            if (maybeException != null) {
                unsafe = null;
                unsafeUnavailabilityCause = (Throwable) maybeException;
            }
        }

        if (unsafe != null) {
            final sun.misc.Unsafe finalUnsafe = unsafe;

            // attempt to access field Buffer#address
            final Object maybeAddressField = AccessController
                .doPrivileged((PrivilegedAction<Object>) () -> {
                    try {
                        final Field field = Buffer.class.getDeclaredField("address");
                        // Use Unsafe to read value of the address field. This way it will
                        // not fail on JDK9+ which
                        // will forbid changing the access level via reflection.
                        final long offset = finalUnsafe.objectFieldOffset(field);
                        final long address = finalUnsafe.getLong(direct, offset);

                        // if direct really is a direct buffer, address will be non-zero
                        if (address == 0) {
                            return null;
                        }
                        return field;
                    } catch (NoSuchFieldException | SecurityException e) {
                        return e;
                    }
                });

            if (maybeAddressField instanceof Field) {
                addressField = (Field) maybeAddressField;
            } else {
                unsafeUnavailabilityCause = (Throwable) maybeAddressField;

                // If we cannot access the address of a direct buffer, there's no point of
                // using unsafe.
                // Let's just pretend unsafe is unavailable for overall simplicity.
                unsafe = null;
            }
        }

        if (unsafe != null) {
            // There are assumptions made where ever BYTE_ARRAY_BASE_OFFSET is used (equals,
            // hashCodeAscii, and
            // primitive accessors) that arrayIndexScale == 1, and results are undefined if
            // this is not the case.
            long byteArrayIndexScale = unsafe.arrayIndexScale(byte[].class);
            if (byteArrayIndexScale != 1) {
                unsafeUnavailabilityCause = new UnsupportedOperationException("Unexpected unsafe.arrayIndexScale");
                unsafe = null;
            }
        }
        UNSAFE_UNAVAILABILITY_CAUSE = unsafeUnavailabilityCause;
        UNSAFE = unsafe;

        if (unsafe == null) {
            ADDRESS_FIELD_OFFSET = -1;
            BYTE_ARRAY_BASE_OFFSET = -1;
            DIRECT_BUFFER_CONSTRUCTOR = null;
        } else {
            Constructor<?> directBufferConstructor;
            long address = -1;
            try {
                final Object maybeDirectBufferConstructor = AccessController
                    .doPrivileged((PrivilegedAction<Object>) () -> {
                        try {
                            final Constructor<?> constructor = direct.getClass()
                                .getDeclaredConstructor(long.class, int.class);
                            Throwable cause = ReflectionUtil.trySetAccessible(constructor, false);
                            if (cause != null) {
                                return cause;
                            }
                            return constructor;
                        } catch (NoSuchMethodException | SecurityException e) {
                            return e;
                        }
                    });

                if (maybeDirectBufferConstructor instanceof Constructor<?>) {
                    address = UNSAFE.allocateMemory(1);
                    // try to use the constructor now
                    try {
                        ((Constructor<?>) maybeDirectBufferConstructor).newInstance(address, 1);
                        directBufferConstructor = (Constructor<?>) maybeDirectBufferConstructor;
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        directBufferConstructor = null;
                    }
                } else {
                    directBufferConstructor = null;
                }
            } finally {
                if (address != -1) {
                    UNSAFE.freeMemory(address);
                }
            }
            DIRECT_BUFFER_CONSTRUCTOR = directBufferConstructor;
            ADDRESS_FIELD_OFFSET = objectFieldOffset(addressField);
            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        }
    }

    private PlatformDependent() {
    }

    static boolean hasUnsafe() {
        return UNSAFE != null;
    }

    static Throwable getUnsafeUnavailabilityCause() {
        return UNSAFE_UNAVAILABILITY_CAUSE;
    }

    public static void throwException(Throwable cause) {
        // JVM has been observed to crash when passing a null argument. See https://github
        // .com/netty/netty/issues/4131.
        UNSAFE.throwException(requireNonNull(cause, "cause"));
    }

    static boolean hasDirectBufferNoCleanerConstructor() {
        return DIRECT_BUFFER_CONSTRUCTOR != null;
    }

    static ByteBuffer allocateDirectNoCleaner(int capacity) {
        // Calling malloc with capacity of 0 may return a null ptr or a memory address that can
        // be used.
        // Just use 1 to make it safe to use in all cases:
        // See: http://pubs.opengroup.org/onlinepubs/009695399/functions/malloc.html
        long addr = UNSAFE.allocateMemory(Math.max(1, capacity));
        return newDirectBuffer(addr, capacity);
    }

    static ByteBuffer newDirectBuffer(long address, int capacity) {
        Preconditions.checkArgument(capacity > 0, "capacity must > 0");

        try {
            return (ByteBuffer) DIRECT_BUFFER_CONSTRUCTOR.newInstance(address, capacity);
        } catch (Throwable cause) {
            // Not expected to ever throw!
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new Error(cause);
        }
    }

    static long directBufferAddress(ByteBuffer buffer) {
        return getLong(buffer, ADDRESS_FIELD_OFFSET);
    }

    static long byteArrayBaseOffset() {
        return BYTE_ARRAY_BASE_OFFSET;
    }

    static Object getObject(Object object, long fieldOffset) {
        return UNSAFE.getObject(object, fieldOffset);
    }

    private static long getLong(Object object, long fieldOffset) {
        return UNSAFE.getLong(object, fieldOffset);
    }

    static long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    static short getShort(long address) {
        return UNSAFE.getShort(address);
    }

    static int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    static void putShort(long address, short value) {
        UNSAFE.putShort(address, value);
    }

    static void putInt(long address, int value) {
        UNSAFE.putInt(address, value);
    }

    static void putLong(long address, long value) {
        UNSAFE.putLong(address, value);
    }

    static void copyMemory(long srcAddr, long dstAddr, long length) {
        // Manual safe-point polling is only needed prior Java9:
        // See https://bugs.openjdk.java.net/browse/JDK-8149596
        if (javaVersion() <= 8) {
            copyMemoryWithSafePointPolling(srcAddr, dstAddr, length);
        } else {
            UNSAFE.copyMemory(srcAddr, dstAddr, length);
        }
    }

    private static void copyMemoryWithSafePointPolling(long srcAddr, long dstAddr, long length) {
        while (length > 0) {
            long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
            UNSAFE.copyMemory(srcAddr, dstAddr, size);
            length -= size;
            srcAddr += size;
            dstAddr += size;
        }
    }

    static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
        UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, length);
    }

    private static void copyMemoryWithSafePointPolling(Object src, long srcOffset, Object dst,
                                                       long dstOffset, long length) {
        while (length > 0) {
            long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
            UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
            length -= size;
            srcOffset += size;
            dstOffset += size;
        }
    }

    static void setMemory(long address, long bytes, byte value) {
        UNSAFE.setMemory(address, bytes, value);
    }

    static ClassLoader getClassLoader(final Class<?> clazz) {
        if (System.getSecurityManager() == null) {
            return clazz.getClassLoader();
        } else {
            return AccessController
                .doPrivileged((PrivilegedAction<ClassLoader>) clazz::getClassLoader);
        }
    }

    static ClassLoader getSystemClassLoader() {
        if (System.getSecurityManager() == null) {
            return ClassLoader.getSystemClassLoader();
        } else {
            return AccessController
                .doPrivileged((PrivilegedAction<ClassLoader>) ClassLoader::getSystemClassLoader);
        }
    }

    static void freeMemory(long address) {
        UNSAFE.freeMemory(address);
    }

    static int javaVersion() {
        return ReflectionUtil.JAVA_VERSION;
    }
}
