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
 * Copyright 2017 The Netty Project
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.geaflow.memory.DirectMemory;
import org.apache.geaflow.memory.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This file is based on source code from the Netty Project (http://netty.io/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Netty's io.netty.util.internal.CleanerJava9.
 * Allows to free direct {@link ByteBuffer} by using Cleaner for Java version equal or greater than 9.
 */
public final class CleanerJava9 implements Cleaner {

    private static final Logger logger = LoggerFactory.getLogger(CleanerJava9.class);

    private static final Method INVOKE_CLEANER;

    static {
        final Method method;
        if (DirectMemory.hasUnsafe()) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(1);
            Object maybeInvokeMethod = AccessController
                .doPrivileged((PrivilegedAction<Object>) () -> {
                    try {
                        // See https://bugs.openjdk.java.net/browse/JDK-8171377
                        Method m = DirectMemory.unsafe().getClass()
                            .getDeclaredMethod("invokeCleaner", ByteBuffer.class);
                        m.invoke(DirectMemory.unsafe(), buffer);
                        return m;
                    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                        return e;
                    }
                });

            if (maybeInvokeMethod instanceof Throwable) {
                method = null;
            } else {
                method = (Method) maybeInvokeMethod;
            }
        } else {
            method = null;
        }
        INVOKE_CLEANER = method;
    }

    public static boolean isSupported() {
        return INVOKE_CLEANER != null;
    }

    private static void freeDirectBufferPrivileged(final ByteBuffer buffer) {
        Exception error = AccessController.doPrivileged((PrivilegedAction<Exception>) () -> {
            try {
                INVOKE_CLEANER.invoke(DirectMemory.unsafe(), buffer);
            } catch (InvocationTargetException | IllegalAccessException e) {
                return e;
            }
            return null;
        });
        if (error != null) {
            PlatformDependent.throwException(error);
        }
    }

    @Override
    public void freeDirectBuffer(ByteBuffer buffer) {
        // Try to minimize overhead when there is no SecurityManager present.
        // See https://bugs.openjdk.java.net/browse/JDK-8191053.
        if (System.getSecurityManager() == null) {
            try {
                INVOKE_CLEANER.invoke(DirectMemory.unsafe(), buffer);
            } catch (Throwable cause) {
                PlatformDependent.throwException(cause);
            }
        } else {
            freeDirectBufferPrivileged(buffer);
        }
    }
}
