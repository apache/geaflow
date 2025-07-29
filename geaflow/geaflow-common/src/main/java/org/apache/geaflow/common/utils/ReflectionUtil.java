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
 * This Class is a modification Class from Netty.
 */

package org.apache.geaflow.common.utils;

import com.google.common.base.Preconditions;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public final class ReflectionUtil {

    public static final int JAVA_VERSION = majorVersion(
        SystemArgsUtil.get("java.specification.version", "1.6"));

    private static int majorVersion(final String javaSpecVersion) {
        final String[] components = javaSpecVersion.split("\\.");
        final int[] version = new int[components.length];
        for (int i = 0; i < components.length; i++) {
            version[i] = Integer.parseInt(components[i]);
        }

        if (version[0] == 1) {
            Preconditions.checkArgument(version[1] >= 6);
            return version[1];
        } else {
            return version[0];
        }
    }

    private ReflectionUtil() {
    }

    /**
     * Set visibility.
     */
    public static Throwable trySetAccessible(AccessibleObject object, boolean checkAccessible) {
        if (checkAccessible && JAVA_VERSION >= 9) {
            return new UnsupportedOperationException("Reflective setAccessible(true) disabled");
        }
        try {
            object.setAccessible(true);
            return null;
        } catch (Exception e) {
            return new GeaflowRuntimeException(e);
        }
    }

    public static Object getField(Object object, String fieldName) {
        try {
            Field field = getField(object.getClass(), fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    private static Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
        while (clazz != Object.class) {
            try {
                return clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                // ignore current class field, try super class.
            }
            clazz = clazz.getSuperclass();
        }
        throw new NoSuchFieldException(fieldName);
    }

    public static void setField(Object instance, String fieldName, Object value) throws Exception {
        Field field = getField(instance.getClass(), fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }
}
