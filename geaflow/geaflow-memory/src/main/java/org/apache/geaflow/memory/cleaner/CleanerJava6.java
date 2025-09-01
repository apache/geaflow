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

/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.memory.cleaner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.geaflow.memory.DirectMemory;
import org.apache.geaflow.memory.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Netty's io.netty.util.internal.CleanerJava6.
 * Allows to free direct {@link ByteBuffer} by using Cleaner for Java version less than 9.
 */
public final class CleanerJava6 implements Cleaner {

    private static final long CLEANER_FIELD_OFFSET;
    private static final Method CLEAN_METHOD;
    private static final Field CLEANER_FIELD;

    private static final Logger logger = LoggerFactory.getLogger(CleanerJava6.class);

    static {
        long fieldOffset;
        Method clean;
        Field cleanerField;
        final ByteBuffer direct = ByteBuffer.allocateDirect(1);
        try {
            Object mayBeCleanerField = AccessController
                .doPrivileged((PrivilegedAction<Object>) () -> {
                    try {
                        Field cleanerField1 = direct.getClass().getDeclaredField("cleaner");
                        if (!DirectMemory.hasUnsafe()) {
                            // We need to make it accessible if we do not use Unsafe as we will
                            // access it via
                            // reflection.
                            cleanerField1.setAccessible(true);
                        }
                        return cleanerField1;
                    } catch (Throwable cause) {
                        return cause;
                    }
                });
            if (mayBeCleanerField instanceof Throwable) {
                throw (Throwable) mayBeCleanerField;
            }

            cleanerField = (Field) mayBeCleanerField;

            final Object cleaner;

            // If we have sun.misc.Unsafe we will use it as its faster then using reflection,
            // otherwise let us try reflection as last resort.
            if (DirectMemory.hasUnsafe()) {
                fieldOffset = DirectMemory.objectFieldOffset(cleanerField);
                cleaner = DirectMemory.getObject(direct, fieldOffset);
            } else {
                fieldOffset = -1;
                cleaner = cleanerField.get(direct);
            }
            clean = cleaner.getClass().getDeclaredMethod("clean");
            clean.invoke(cleaner);
        } catch (Throwable t) {
            // We don't have ByteBuffer.cleaner().
            fieldOffset = -1;
            clean = null;
            cleanerField = null;
        }

        CLEANER_FIELD = cleanerField;
        CLEANER_FIELD_OFFSET = fieldOffset;
        CLEAN_METHOD = clean;
    }

    public static boolean isSupported() {
        return CLEANER_FIELD_OFFSET != -1 || CLEANER_FIELD != null;
    }

    private static void freeDirectBufferPrivileged(final ByteBuffer buffer) {
        Throwable cause = AccessController.doPrivileged((PrivilegedAction<Throwable>) () -> {
            try {
                freeDirectBuffer0(buffer);
                return null;
            } catch (Throwable cause1) {
                return cause1;
            }
        });
        if (cause != null) {
            PlatformDependent.throwException(cause);
        }
    }

    private static void freeDirectBuffer0(ByteBuffer buffer) throws Exception {
        final Object cleaner;
        // If CLEANER_FIELD_OFFSET == -1 we need to use reflection to access the cleaner,
        // otherwise we can use
        // sun.misc.Unsafe.
        if (CLEANER_FIELD_OFFSET == -1) {
            cleaner = CLEANER_FIELD.get(buffer);
        } else {
            cleaner = DirectMemory.getObject(buffer, CLEANER_FIELD_OFFSET);
        }
        if (cleaner != null) {
            CLEAN_METHOD.invoke(cleaner);
        }
    }

    @Override
    public void freeDirectBuffer(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            return;
        }
        if (System.getSecurityManager() == null) {
            try {
                freeDirectBuffer0(buffer);
            } catch (Throwable cause) {
                PlatformDependent.throwException(cause);
            }
        } else {
            freeDirectBufferPrivileged(buffer);
        }
    }
}
