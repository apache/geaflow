/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.memory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.geaflow.common.utils.SystemArgsUtil;
import org.apache.geaflow.memory.cleaner.Cleaner;
import org.apache.geaflow.memory.cleaner.CleanerJava6;
import org.apache.geaflow.memory.cleaner.CleanerJava9;
import org.apache.geaflow.memory.exception.GeaflowOutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Netty's io.netty.util.internal.DirectMemory.
 */
public final class DirectMemory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectMemory.class);

    private static final Pattern MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN = Pattern
        .compile("\\s*-XX:MaxDirectMemorySize\\s*=\\s*([0-9]+)\\s*([kKmMgG]?)\\s*$");
    private static final Throwable UNSAFE_UNAVAILABILITY_CAUSE = unsafeUnavailabilityCause0();
    private static final long MAX_DIRECT_MEMORY = maxDirectMemory0();
    private static final long BYTE_ARRAY_BASE_OFFSET = byteArrayBaseOffset0();
    private static final boolean USE_DIRECT_BUFFER_NO_CLEANER;
    private static final AtomicLong DIRECT_MEMORY_COUNTER;
    private static final long DIRECT_MEMORY_LIMIT;
    private static final Cleaner CLEANER;
    private static final Cleaner NOOP = buffer -> {
        // NOOP
    };

    static {
        // Here is how the system property is used:
        //
        // * <  0  - Don't use cleaner, and inherit max direct memory from java. In this case the
        //           "practical max direct memory" would be 2 * max memory as defined by the JDK.
        // * == 0  - Use cleaner, Netty will not enforce max memory, and instead will defer to JDK.
        // * >  0  - Don't use cleaner. This will limit Netty's total direct memory
        //           (note: that JDK's direct memory limit is independent of this).
        long maxDirectMemory = -1;

        if (!hasUnsafe() || !PlatformDependent
            .hasDirectBufferNoCleanerConstructor()) {
            USE_DIRECT_BUFFER_NO_CLEANER = false;
            DIRECT_MEMORY_COUNTER = null;
        } else {
            USE_DIRECT_BUFFER_NO_CLEANER = true;
            maxDirectMemory = MAX_DIRECT_MEMORY;
            if (maxDirectMemory <= 0) {
                DIRECT_MEMORY_COUNTER = null;
            } else {
                DIRECT_MEMORY_COUNTER = new AtomicLong();
            }
        }
        DIRECT_MEMORY_LIMIT = maxDirectMemory >= 1 ? maxDirectMemory : MAX_DIRECT_MEMORY;

        // only direct to method if we are not running on android.
        // See https://github.com/netty/netty/issues/2604
        if (javaVersion() >= 9) {
            CLEANER = CleanerJava9.isSupported() ? new CleanerJava9() : NOOP;
        } else {
            CLEANER = CleanerJava6.isSupported() ? new CleanerJava6() : NOOP;
        }

        /*
         * We do not want to log this message if unsafe is explicitly disabled. Do not remove the
         * explicit no unsafe
         * guard.
         */
        if (CLEANER == NOOP) {
            LOGGER.info(
                "Your platform does not provide complete low-level API for accessing direct buffers reliably. "
                , "Unless explicitly requested, heap buffer will always be preferred to avoid potential system instability.");
        }
    }

    private DirectMemory() {
        // only static method supported
    }

    /**
     * Return the version of Java under which this library is used.
     */
    public static int javaVersion() {
        return PlatformDependent.javaVersion();
    }

    /**
     * Return {@code true} if {@code sun.misc.Unsafe} was found on the classpath and can be used for
     * accelerated.
     * direct memory access.
     */
    public static boolean hasUnsafe() {
        return UNSAFE_UNAVAILABILITY_CAUSE == null;
    }

    /**
     * Raises an exception bypassing compiler checks for checked exceptions.
     */
    public static void throwException(Throwable t) {
        if (hasUnsafe()) {
            PlatformDependent.throwException(t);
        } else {
            DirectMemory.throwException0(t);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwException0(Throwable t) throws E {
        throw (E) t;
    }

    /**
     * Try to deallocate the specified direct {@link ByteBuffer}. Please note this method does
     * nothing if
     * the current platform does not support this operation or the specified buffer is not a direct
     * buffer.
     */
    public static void freeDirectBuffer(ByteBuffer buffer) {
        CLEANER.freeDirectBuffer(buffer);
    }

    public static long directBufferAddress(ByteBuffer buffer) {
        return PlatformDependent.directBufferAddress(buffer);
    }

    public static Object getObject(Object object, long fieldOffset) {
        return PlatformDependent.getObject(object, fieldOffset);
    }


    public static byte getByte(long address) {
        return PlatformDependent.getByte(address);
    }

    public static short getShort(long address) {
        return PlatformDependent.getShort(address);
    }

    public static int getInt(long address) {
        return PlatformDependent.getInt(address);
    }

    public static long getLong(long address) {
        return PlatformDependent.getLong(address);
    }

    public static void putByte(long address, byte value) {
        PlatformDependent.putByte(address, value);
    }

    public static void putShort(long address, short value) {
        PlatformDependent.putShort(address, value);
    }

    public static void putInt(long address, int value) {
        PlatformDependent.putInt(address, value);
    }

    public static void putLong(long address, long value) {
        PlatformDependent.putLong(address, value);
    }

    public static long objectFieldOffset(Field field) {
        return PlatformDependent.objectFieldOffset(field);
    }

    public static void copyMemory(long srcAddr, long dstAddr, long length) {
        PlatformDependent.copyMemory(srcAddr, dstAddr, length);
    }

    public static void copyMemory(long srcAddr, byte[] dst, int dstIndex, long length) {
        PlatformDependent.copyMemory(null, srcAddr, dst, BYTE_ARRAY_BASE_OFFSET + dstIndex, length);
    }

    public static void copyMemory(byte[] src, int srcIndex, long dstAddr, long length) {
        PlatformDependent.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + srcIndex, null, dstAddr, length);
    }

    public static void freeMemory(long address) {
        PlatformDependent.freeMemory(address);
    }

    public static void setMemory(long address, long bytes, byte value) {
        PlatformDependent.setMemory(address, bytes, value);
    }

    static ByteBuffer[] splitBuffer(ByteBuffer byteBuffer, int len) {
        long address = directBufferAddress(byteBuffer);
        int inSize = byteBuffer.capacity() / len;
        ByteBuffer[] bfs = new ByteBuffer[inSize];
        int offset = 0;

        for (int i = 0; i < inSize; i++) {
            bfs[i] = PlatformDependent.newDirectBuffer(address + offset, len);
            offset += len;
        }
        return bfs;
    }

    static ByteBuffer mergeBuffer(ByteBuffer bf1, ByteBuffer bf2) {
        return PlatformDependent
            .newDirectBuffer(directBufferAddress(bf1), bf1.capacity() + bf2.capacity());
    }

    /**
     * Allocate a new {@link ByteBuffer} with the given {@code capacity}. {@link ByteBuffer}s
     * allocated with
     * this method <strong>MUST</strong> be deallocated via
     * {@link #freeDirectNoCleaner(ByteBuffer)}.
     */
    public static ByteBuffer allocateDirectNoCleaner(int capacity) {
        assert USE_DIRECT_BUFFER_NO_CLEANER;

        incrementMemoryCounter(capacity);
        try {
            return PlatformDependent.allocateDirectNoCleaner(capacity);
        } catch (Throwable e) {
            decrementMemoryCounter(capacity);
            throwException(e);
            return null;
        }
    }

    /**
     * This method <strong>MUST</strong> only be called for {@link ByteBuffer}s that were allocated
     * via
     * {@link #allocateDirectNoCleaner(int)}.
     */
    public static void freeDirectNoCleaner(ByteBuffer buffer) {
        assert USE_DIRECT_BUFFER_NO_CLEANER;

        int capacity = buffer.capacity();
        PlatformDependent.freeMemory(PlatformDependent.directBufferAddress(buffer));
        decrementMemoryCounter(capacity);
    }

    private static void incrementMemoryCounter(int capacity) {
        if (DIRECT_MEMORY_COUNTER != null) {
            long newUsedMemory = DIRECT_MEMORY_COUNTER.addAndGet(capacity);
            if (newUsedMemory > DIRECT_MEMORY_LIMIT) {
                DIRECT_MEMORY_COUNTER.addAndGet(-capacity);
                throw new GeaflowOutOfMemoryException(
                    "failed to allocate " + capacity + " byte(s) of direct memory (used: " + (
                        newUsedMemory - capacity) + ", max: " + DIRECT_MEMORY_LIMIT + ')');
            }
        }
    }

    private static void decrementMemoryCounter(int capacity) {
        if (DIRECT_MEMORY_COUNTER != null) {
            long usedMemory = DIRECT_MEMORY_COUNTER.addAndGet(-capacity);
            assert usedMemory >= 0;
        }
    }

    public static boolean useDirectBufferNoCleaner() {
        return USE_DIRECT_BUFFER_NO_CLEANER;
    }

    public static sun.misc.Unsafe unsafe() {
        return PlatformDependent.UNSAFE;
    }

    /**
     * Return the system {@link ClassLoader}.
     */
    public static ClassLoader getSystemClassLoader() {
        return PlatformDependent.getSystemClassLoader();
    }

    private static Throwable unsafeUnavailabilityCause0() {
        Throwable cause = PlatformDependent.getUnsafeUnavailabilityCause();
        if (cause != null) {
            return cause;
        }

        try {
            boolean hasUnsafe = PlatformDependent.hasUnsafe();
            LOGGER.debug("sun.misc.Unsafe: {}", hasUnsafe ? "available" : "unavailable");
            return hasUnsafe ? null : PlatformDependent.getUnsafeUnavailabilityCause();
        } catch (Throwable t) {
            LOGGER.trace("Could not determine if Unsafe is available", t);
            // Probably failed to initialize PlatformDependent0.
            return new UnsupportedOperationException("Could not determine if Unsafe is available",
                t);
        }
    }

    public static long maxDirectMemory0() {
        long maxDirectMemory = 0;

        ClassLoader systemClassLoader = null;
        try {
            systemClassLoader = getSystemClassLoader();

            // When using IBM J9 / Eclipse OpenJ9 we should not use VM.maxDirectMemory() as it
            // not reflects the
            // correct value.
            // See:
            //  - https://github.com/netty/netty/issues/7654
            String vmName = SystemArgsUtil.get("java.vm.name", "").toLowerCase();
            if (!vmName.startsWith("ibm j9")
                // https://github.com/eclipse/openj9/blob/openj9-0.8
                // .0/runtime/include/vendor_version.h#L53
                && !vmName.startsWith("eclipse openj9")) {
                // Try to build from sun.misc.VM.maxDirectMemory() which should be most accurate.
                Class<?> vmClass = Class.forName("sun.misc.VM", true, systemClassLoader);
                Method m = vmClass.getDeclaredMethod("maxDirectMemory");
                maxDirectMemory = ((Number) m.invoke(null)).longValue();
            }
        } catch (Throwable ignored) {
            LOGGER.warn("fail maxDirectMemory0", ignored);
        }

        if (maxDirectMemory > 0) {
            LOGGER.info("maxDirectMemory: {} bytes from sun.misc.VM", maxDirectMemory);
            return maxDirectMemory;
        }

        List<String> vmArgs = null;
        try {
            // Now try to build the JVM option (-XX:MaxDirectMemorySize) and parse it.
            // Note that we are using reflection because Android doesn't have these classes.
            Class<?> mgmtFactoryClass = Class
                .forName("java.lang.management.ManagementFactory", true, systemClassLoader);
            Class<?> runtimeClass = Class
                .forName("java.lang.management.RuntimeMXBean", true, systemClassLoader);

            Object runtime = mgmtFactoryClass.getDeclaredMethod("getRuntimeMXBean").invoke(null);

            vmArgs = (List<String>) runtimeClass
                .getDeclaredMethod("getInputArguments").invoke(runtime);
        } catch (Throwable ignored) {
            LOGGER.warn("fail maxDirectMemory0", ignored);
        }

        return maxDirectMemoryFromJVMOption(vmArgs);
    }

    public static long maxDirectMemoryFromJVMOption(List<String> vmArgs) {
        long maxDirectMemory = 0;
        try {
            for (int i = vmArgs.size() - 1; i >= 0; i--) {
                Matcher m = MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN.matcher(vmArgs.get(i));
                if (!m.matches()) {
                    continue;
                }

                maxDirectMemory = Long.parseLong(m.group(1));
                switch (m.group(2).charAt(0)) {
                    case 'k':
                    case 'K':
                        maxDirectMemory *= 1024;
                        break;
                    case 'm':
                    case 'M':
                        maxDirectMemory *= 1024 * 1024;
                        break;
                    case 'g':
                    case 'G':
                        maxDirectMemory *= 1024 * 1024 * 1024;
                        break;
                    default:
                        throw new IllegalAccessException();
                }
                break;
            }
        } catch (Throwable ignored) {
            LOGGER.warn("fail maxDirectMemory0", ignored);
        }

        if (maxDirectMemory <= 0) {
            maxDirectMemory = Runtime.getRuntime().maxMemory();
            LOGGER.info("maxDirectMemory: {} bytes (maybe) from jvm", maxDirectMemory);
        } else {
            LOGGER.info("maxDirectMemory: {} bytes from args", maxDirectMemory);
        }

        return maxDirectMemory;
    }

    private static long byteArrayBaseOffset0() {
        if (!hasUnsafe()) {
            return -1;
        }
        return PlatformDependent.byteArrayBaseOffset();
    }
}
