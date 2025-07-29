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

package org.apache.geaflow.console.common.util;

public class LoaderSwitchUtil {

    public static void run(ClassLoader classLoader, Runnable runnable) throws Exception {
        Thread currentThread = Thread.currentThread();
        ClassLoader oldClassLoader = currentThread.getContextClassLoader();

        try {
            currentThread.setContextClassLoader(classLoader);
            runnable.run();

        } finally {
            currentThread.setContextClassLoader(oldClassLoader);
        }
    }

    public static <T> T call(ClassLoader classLoader, Callable<T> callable) throws Exception {
        Thread currentThread = Thread.currentThread();
        ClassLoader oldClassLoader = currentThread.getContextClassLoader();

        try {
            currentThread.setContextClassLoader(classLoader);
            return callable.call();

        } finally {
            currentThread.setContextClassLoader(oldClassLoader);
        }
    }

    public interface Runnable {

        void run() throws Exception;
    }

    public interface Callable<T> {

        T call() throws Exception;
    }

}
