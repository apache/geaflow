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

package org.apache.geaflow.console.common.util.proxy;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.geaflow.console.common.util.LoaderSwitchUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

public class ProxyUtil {

    public static <T> T newInstance(ClassLoader targetClassLoader, Class<T> clazz, Object... args) {
        try {
            return LoaderSwitchUtil.call(targetClassLoader, () -> {
                ProxyClass proxyClass = clazz.getAnnotation(ProxyClass.class);
                if (proxyClass == null) {
                    throw new GeaflowException("Use @ProxyClass on class {}", clazz.getSimpleName());
                }

                String targetClassName = proxyClass.value();
                Class<?> targetClass = targetClassLoader.loadClass(targetClassName);

                Object[] targetArgs = getTargetArgs(args);
                Constructor<?> constructor = getConstructor(targetClass, targetArgs);
                Object targetInstance = constructor.newInstance(targetArgs);

                return clazz.cast(proxyInstance(targetClassLoader, clazz, targetInstance));
            });

        } catch (Exception e) {
            throw new GeaflowException("Create instance of {} failed", clazz.getSimpleName(), e);
        }
    }

    protected static Constructor<?> getConstructor(Class<?> clazz, Object[] args) {
        Constructor<?>[] constructors = clazz.getConstructors();
        for (Constructor<?> constructor : constructors) {
            // parameter count check
            if (constructor.getParameterCount() != args.length) {
                continue;
            }

            boolean matched = true;
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            for (int i = 0; i < parameterTypes.length; i++) {
                Object targetArg = args[i];

                if (targetArg == null) {
                    // primitive type not allowed null
                    if (!Object.class.isAssignableFrom(parameterTypes[i])) {
                        matched = false;
                        break;
                    }

                } else {
                    // type compatible
                    if (!parameterTypes[i].isAssignableFrom(targetArg.getClass())) {
                        matched = false;
                        break;
                    }
                }
            }

            if (matched) {
                return constructor;
            }
        }

        throw new GeaflowException("No compatible constructor found of {}", clazz.getSimpleName());
    }

    protected static Object[] getTargetArgs(Object[] args) {
        if (args == null) {
            return null;
        }

        Object[] targetArgs = args.clone();
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg instanceof Proxy) {
                targetArgs[i] = ((GeaflowInvocationHandler) Proxy.getInvocationHandler(arg)).getTargetInstance();
            }
        }

        return targetArgs;
    }

    protected static Method getTargetMethod(Method method, Class<?> targetClass) {
        try {
            ClassLoader targetClassLoader = targetClass.getClassLoader();
            Class<?>[] parameterTypes = method.getParameterTypes();

            // cast to target parameter types
            Class<?>[] targetParameterTypes = parameterTypes.clone();
            for (int i = 0; i < targetParameterTypes.length; i++) {
                ProxyClass targetParameterClass = targetParameterTypes[i].getAnnotation(ProxyClass.class);
                if (targetParameterClass != null) {
                    targetParameterTypes[i] = targetClassLoader.loadClass(targetParameterClass.value());
                }
            }

            // get target method
            return targetClass.getMethod(method.getName(), targetParameterTypes);

        } catch (Exception e) {
            throw new GeaflowException("Get target method {} failed", method.getName(), e);
        }
    }

    protected static Object proxyInstance(ClassLoader targetClassLoader, Type type, Object targetInstance) {
        if (type instanceof Class) {
            Class<?> clazz = (Class<?>) type;

            // primitive type
            if (clazz.isPrimitive()) {
                return targetInstance;
            }

            // cast directly
            if (clazz.getAnnotation(ProxyClass.class) == null || targetInstance == null) {
                return clazz.cast(targetInstance);
            }

            // proxy instance
            Object proxyInstance = Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz},
                new GeaflowInvocationHandler(targetClassLoader, targetInstance));
            return clazz.cast(proxyInstance);
        }

        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Class<?> clazz = (Class<?>) parameterizedType.getRawType();
            Type[] elementType = parameterizedType.getActualTypeArguments();

            // proxy collection type
            if (Collection.class.isAssignableFrom(clazz)) {
                return proxyCollection(targetClassLoader, clazz, elementType[0], targetInstance);
            }

            // proxy map type
            if (Map.class.isAssignableFrom(clazz)) {
                return proxyMap(targetClassLoader, elementType[0], elementType[1], targetInstance);
            }

            // proxy raw type
            return proxyInstance(targetClassLoader, clazz, targetInstance);
        }

        throw new GeaflowException("Type {} not support proxy", type.getTypeName());
    }

    private static Object proxyCollection(ClassLoader targetClassLoader, Class<?> clazz, Type elementType,
                                          Object targetInstance) {
        // create default collection
        Collection<Object> proxyCollection;
        if (Set.class.isAssignableFrom(clazz)) {
            proxyCollection = new HashSet<>();

        } else if (List.class.isAssignableFrom(clazz)) {
            proxyCollection = new ArrayList<>();

        } else {
            throw new GeaflowException("Result collection class {} not supported", clazz.getSimpleName());
        }

        // proxy collection element
        Collection<?> targetCollection = (Collection<?>) targetInstance;
        for (Object element : targetCollection) {
            proxyCollection.add(proxyInstance(targetClassLoader, elementType, element));
        }

        return proxyCollection;
    }

    private static Object proxyMap(ClassLoader targetClassLoader, Type keyType, Type valueType, Object targetInstance) {
        // create proxy map
        Map<Object, Object> proxyMap = new HashMap<>();

        // proxy map key value
        Map<?, ?> targetMap = (Map<?, ?>) targetInstance;
        for (Entry<?, ?> entry : targetMap.entrySet()) {
            Object proxyKey = proxyInstance(targetClassLoader, keyType, entry.getKey());
            Object proxyValue = proxyInstance(targetClassLoader, valueType, entry.getValue());
            proxyMap.put(proxyKey, proxyValue);
        }

        return proxyMap;
    }
}
