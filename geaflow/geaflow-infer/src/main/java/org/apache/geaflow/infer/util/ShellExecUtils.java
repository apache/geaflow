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

package org.apache.geaflow.infer.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.log.ProcessErrorOutputLogger;
import org.apache.geaflow.infer.log.ProcessStdOutputLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShellExecUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShellExecUtils.class);

    private static final String SHELL_KEY = "sh";

    private static final String SHELL_PARAM = "-c";

    private static final Consumer<String> DUMMY_CONSUMER = s -> {
    };

    private static final ExecutorService LOGGER_POOL =
        new ThreadPoolExecutor(
            2,
            20,
            10,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            new ThreadFactoryBuilder()
                .setNameFormat("infer-task-log-%d")
                .setDaemon(true)
                .build());

    private ShellExecUtils() {
    }


    public static boolean run(String cmd, Consumer<String> stdOutputConsumer,
                              Consumer<String> errOutputConsumer, Duration timeout,
                              boolean allowFailure, String workingDir) {
        ProcessBuilder builder = new ProcessBuilder(SHELL_KEY, SHELL_PARAM, cmd);
        if (workingDir != null) {
            builder.directory(new File(workingDir));
        }
        Process process = null;
        int exitCode = 0;
        try {
            process = builder.start();
            if (stdOutputConsumer == null) {
                stdOutputConsumer = DUMMY_CONSUMER;
            }
            if (errOutputConsumer == null) {
                errOutputConsumer = DUMMY_CONSUMER;
            }
            ProcessErrorOutputLogger processErrorOutputLogger =
                new ProcessErrorOutputLogger(process.getErrorStream(), errOutputConsumer);
            LOGGER_POOL.execute(new ProcessStdOutputLogger(process.getInputStream(), stdOutputConsumer));
            LOGGER_POOL.execute(processErrorOutputLogger);
            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            exitCode = process.exitValue();
            boolean success = finished && exitCode == 0;
            if (!success && !allowFailure) {
                if (!finished) {
                    throw new GeaflowRuntimeException(String.format("Command %s didn't finish in "
                        + "time, please try increase the "
                        + "timeout %s", cmd, FrameworkConfigKeys.INFER_ENV_INIT_TIMEOUT_SEC));
                } else {
                    LOGGER.error("Command {} exec failed, error message {}", cmd, processErrorOutputLogger.get());
                }
            }
            return success;
        } catch (Exception e) {
            LOGGER.error("error running {}, exit code is {}", cmd, exitCode, e);
            throw new GeaflowRuntimeException("running shell exception", e);
        } finally {
            if (process != null) {
                process.destroyForcibly();
            }
        }
    }

    public static boolean run(String cmd, Duration timeout, Consumer<String> stdOutputConsumer,
                              Consumer<String> errOutputConsumer) {
        return run(cmd, stdOutputConsumer, errOutputConsumer,
            timeout, false, null);
    }

    public static boolean run(String cmd, Duration timeout,
                              Consumer<String> stdOutputConsumer,
                              Consumer<String> errOutputConsumer, String workDir) {
        return run(cmd, stdOutputConsumer, errOutputConsumer,
            timeout, false, workDir);
    }

}
