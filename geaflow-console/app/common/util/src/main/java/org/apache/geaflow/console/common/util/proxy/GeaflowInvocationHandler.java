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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.geaflow.console.common.util.LoaderSwitchUtil;

@Getter
@AllArgsConstructor
public class GeaflowInvocationHandler implements InvocationHandler {

    private final ClassLoader targetClassLoader;

    private final Object targetInstance;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            Class<?> targetClass = targetInstance.getClass();

            return LoaderSwitchUtil.call(targetClassLoader, () -> {
                // prepare target args
                Object[] targetArgs = ProxyUtil.getTargetArgs(args);

                // prepare target method
                Method targetMethod = ProxyUtil.getTargetMethod(method, targetClass);

                // invoke target method
                Object targetResult = targetMethod.invoke(targetInstance, targetArgs);

                // proxy target result
                return ProxyUtil.proxyInstance(targetClassLoader, method.getGenericReturnType(), targetResult);
            });
        } catch (Exception e) {
            if (e instanceof InvocationTargetException) {
                throw ((InvocationTargetException) e).getTargetException();
            }
            throw e;
        }
    }
}
