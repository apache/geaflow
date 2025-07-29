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

package org.apache.geaflow.common.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorUtil.class);

    private static final int SHUTDOWN_TIMEOUT_MS = 1000;

    public static void execute(ExecutorService service, Runnable command, ExceptionHandler handler) {
        service.execute(() -> {
            try {
                command.run();
            } catch (Throwable throwable) {
                handler.handle(throwable);
            }
        });
    }

    public static void spinLockMs(Supplier<Boolean> condition, Runnable checkFun, long ms) {
        while (!condition.get()) {
            SleepUtils.sleepMilliSecond(ms);
            checkFun.run();
        }
    }

    public static void shutdown(ExecutorService executorService, long timeout, TimeUnit timeUnit) {
        LOGGER.info("shutdown executor service {}", executorService);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeout, timeUnit)) {
                LOGGER.info("shutdown executor service force");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted when shutdown executor service", e);
        }
    }

    public static void shutdown(ExecutorService executorService) {
        shutdown(executorService, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public interface ExceptionHandler {
        void handle(Throwable exp);
    }

}
