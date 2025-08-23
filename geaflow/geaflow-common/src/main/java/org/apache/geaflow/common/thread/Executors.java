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

package org.apache.geaflow.common.thread;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

public class Executors {

    private static final int DEFAULT_KEEP_ALIVE_MINUTES = 30;
    private static final int DEFAULT_QUEUE_CAPACITY = 1024;
    private static final int DEFAULT_MAGNIFICATION = 2;
    private static final int DEFAULT_MAX_MULTIPLE = 10;

    private static final Map<String, ExecutorService> BOUNDED_EXECUTORS = new HashMap<>();
    private static final Map<String, ExecutorService> UNBOUNDED_EXECUTORS = new HashMap<>();
    private static final int CORE_NUM = Runtime.getRuntime().availableProcessors();

    private static String getKey(String type, int bound, int capacity, long keepAliveTime,
                                 TimeUnit unit) {
        return String.format("%s%s%s%s%s", type, bound, capacity, keepAliveTime, unit);
    }

    public static synchronized ExecutorService getBoundedService(int bound, int capacity,
                                                                 long keepAliveTime,
                                                                 TimeUnit unit) {
        String key = getKey("bound", bound, capacity, keepAliveTime, unit);
        if (BOUNDED_EXECUTORS.get(key) == null) {
            BoundedExecutor boundedExecutor = new BoundedExecutor(bound, capacity, keepAliveTime,
                unit);
            BOUNDED_EXECUTORS.put(key, boundedExecutor);
        }
        return BOUNDED_EXECUTORS.get(key);
    }

    public static synchronized ExecutorService getMaxCoreBoundedService() {
        return getMaxCoreBoundedService(DEFAULT_MAGNIFICATION);
    }

    public static synchronized ExecutorService getMaxCoreBoundedService(int magnification) {
        int cores = Runtime.getRuntime().availableProcessors();
        return getBoundedService(magnification * cores, DEFAULT_QUEUE_CAPACITY,
            DEFAULT_KEEP_ALIVE_MINUTES, TimeUnit.MINUTES);
    }

    public static synchronized ExecutorService getService(int bound, int capacity,
                                                          long keepAliveTime, TimeUnit unit) {
        String key = getKey("normal", bound, capacity, keepAliveTime, unit);
        if (BOUNDED_EXECUTORS.get(key) == null) {
            ExecutorService boundedExecutor = new ThreadPoolExecutor(bound, bound, keepAliveTime,
                unit, new LinkedBlockingQueue<>(capacity));
            BOUNDED_EXECUTORS.put(key, boundedExecutor);
        }
        return BOUNDED_EXECUTORS.get(key);
    }

    public static synchronized ExecutorService getMultiCoreExecutorService(int maxMultiple,
                                                                           double magnification) {
        return getExecutorService(maxMultiple, (int) (magnification * CORE_NUM));
    }

    public static synchronized ExecutorService getExecutorService(int maxMultiple, int coreNumber) {
        Preconditions.checkArgument(coreNumber > 0 && coreNumber <= maxMultiple * CORE_NUM,
            "executor core not right " + coreNumber + " is greater than " + maxMultiple * CORE_NUM);
        return getService(coreNumber, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE_MINUTES,
            TimeUnit.MINUTES);
    }

    public static ExecutorService getExecutorService(int coreNumber,
                                                     String threadFormat) {
        return getExecutorService(DEFAULT_MAX_MULTIPLE, coreNumber, threadFormat, null);
    }

    public static ExecutorService getExecutorService(int coreNumber,
                                                     String threadFormat,
                                                     Thread.UncaughtExceptionHandler handler) {
        return getExecutorService(DEFAULT_MAX_MULTIPLE, coreNumber, threadFormat, handler);
    }

    public static ExecutorService getExecutorService(int maxMultiple,
                                                     int coreNumber,
                                                     String threadFormat) {
        return getExecutorService(maxMultiple, coreNumber, threadFormat, null);
    }

    /**
     * Creates an ExecutorService with following params.
     *
     * @param maxMultiple  Maximum threads multiplier
     * @param coreNumber   Number of core threads
     * @param threadFormat Thread name format
     * @param handler      Uncaught exception handler
     * @return Configured ExecutorService
     */
    public static synchronized ExecutorService getExecutorService(int maxMultiple,
                                                                  int coreNumber,
                                                                  String threadFormat,
                                                                  Thread.UncaughtExceptionHandler handler) {
        int maxThreads = maxMultiple * CORE_NUM;
        Preconditions.checkArgument(coreNumber > 0 && coreNumber <= maxThreads,
            "executor threads should be smaller than " + maxThreads);
        Preconditions.checkArgument(StringUtils.isNotEmpty(threadFormat),
            "thread format couldn't" + " be empty");
        return getNamedService(coreNumber, Integer.MAX_VALUE, DEFAULT_KEEP_ALIVE_MINUTES,
            TimeUnit.MINUTES, threadFormat, handler);
    }

    private static synchronized ExecutorService getNamedService(int bound, int capacity,
                                                                long keepAliveTime, TimeUnit unit,
                                                                String threadFormat,
                                                                Thread.UncaughtExceptionHandler handler) {
        String key = getKey(threadFormat, bound, capacity, keepAliveTime, unit);
        if (BOUNDED_EXECUTORS.get(key) == null || BOUNDED_EXECUTORS.get(key).isShutdown()) {
            ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
                .setNameFormat(threadFormat)
                .setDaemon(true);
            if (handler != null) {
                builder.setUncaughtExceptionHandler(handler);
            }
            ExecutorService boundedExecutor = new ThreadPoolExecutor(bound, bound, keepAliveTime,
                unit, new LinkedBlockingQueue<>(capacity), builder.build());
            BOUNDED_EXECUTORS.put(key, boundedExecutor);
        }
        return BOUNDED_EXECUTORS.get(key);
    }

    public static synchronized ExecutorService getUnboundedExecutorService(String name,
                                                                           long keepAliveTime,
                                                                           TimeUnit unit,
                                                                           String threadFormat,
                                                                           Thread.UncaughtExceptionHandler handler) {
        ExecutorService cached = UNBOUNDED_EXECUTORS.get(name);
        if (cached != null && !cached.isShutdown()) {
            return cached;
        }

        ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
            .setDaemon(true);
        if (threadFormat != null) {
            builder.setNameFormat(threadFormat);
        }
        if (handler != null) {
            builder.setUncaughtExceptionHandler(handler);
        }
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE, keepAliveTime, unit,
            new SynchronousQueue<>(), builder.build());
        UNBOUNDED_EXECUTORS.put(name, pool);
        return pool;

    }

}
