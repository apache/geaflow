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
package org.apache.geaflow.infer;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_ENV_CONDA_URL;
import static org.apache.geaflow.infer.util.InferFileUtils.releaseLock;

import com.google.common.base.Joiner;
import java.io.File;
import java.nio.channels.FileLock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.util.InferFileUtils;
import org.apache.geaflow.infer.util.ShellExecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferEnvironmentManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferEnvironmentManager.class);

    private static final String LOCK_FILE = "_lock";

    private static final String SHELL_START = "/bin/bash";

    private static final long TIMEOUT_SECOND = 10;

    private static final String SCRIPT_SEPARATOR = " ";

    private static final String CHMOD_CMD = "chmod";

    private static final String CHMOD_PERMISSION = "755";

    private static final String FINISH_FILE = "_finish";

    private static final String FAILED_FILE = "_failed";

    private static final String EXEC_POOL_PREFIX = "create-infer-env-";

    private static final String VIRTUAL_ENV_DIR = "inferEnv";

    private static final String INFER_FILES_DIR = "inferFiles";

    private static final AtomicReference<Throwable> ERROR_CASE = new AtomicReference<>();

    private static final AtomicInteger THREAD_IDX_GENERATOR = new AtomicInteger();

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

    private static final AtomicBoolean SUCCESS_FLAG = new AtomicBoolean(false);

    private static InferEnvironmentManager INSTANCE;

    private static InferEnvironmentContext environmentContext;

    private final Configuration configuration;

    private final transient ExecutorService executorService;


    public static synchronized InferEnvironmentManager buildInferEnvironmentManager(Configuration config) {
        if (INSTANCE == null) {
            INSTANCE = new InferEnvironmentManager(config);
        }
        return INSTANCE;
    }

    private InferEnvironmentManager(Configuration config) {
        this.configuration = config;
        this.executorService = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            String name = String.format(EXEC_POOL_PREFIX + THREAD_IDX_GENERATOR.getAndIncrement());
            t.setName(name);
            t.setDaemon(true);
            return t;
        });
    }

    public void createEnvironment() {
        if (INITIALIZED.compareAndSet(false, true)) {
            executorService.execute(() -> {
                try {
                    environmentContext = constructInferEnvironment(configuration);
                    if (environmentContext.enableFinished()) {
                        SUCCESS_FLAG.set(true);
                        LOGGER.info("{} create infer environment finished",
                            environmentContext.getRoleNameIndex());
                    }
                } catch (Throwable e) {
                    SUCCESS_FLAG.set(false);
                    ERROR_CASE.set(e);
                    LOGGER.error("execute install infer environment error", e);
                }
            });
        }
    }

    private InferEnvironmentContext constructInferEnvironment(Configuration configuration) {
        String inferEnvDirectory = InferFileUtils.createTargetDir(VIRTUAL_ENV_DIR, configuration);
        String inferFilesDirectory = InferFileUtils.createTargetDir(INFER_FILES_DIR, configuration);

        InferEnvironmentContext environmentContext =
            new InferEnvironmentContext(inferEnvDirectory, inferFilesDirectory, configuration);
        File lockFile;
        FileLock lock = null;
        try {
            lockFile = new File(inferEnvDirectory + File.separator + LOCK_FILE);
            if (!lockFile.exists()) {
                boolean createLock = lockFile.createNewFile();
                LOGGER.info("{} create lock file result {}",
                    environmentContext.getRoleNameIndex(), createLock);
            }

            lock = InferFileUtils.addLock(lockFile);
            File finishFile = new File(inferEnvDirectory + File.separator + FINISH_FILE);
            File failedFile = new File(inferEnvDirectory + File.separator + FAILED_FILE);
            if (failedFile.exists()) {
                environmentContext.setFinished(false);
                LOGGER.warn("{} create infer environment failed", environmentContext.getRoleNameIndex());
                return environmentContext;
            }
            if (finishFile.exists()) {
                environmentContext.setFinished(true);
                LOGGER.info("{} create infer environment finished", environmentContext.getRoleNameIndex());
                return environmentContext;
            }
            InferDependencyManager inferDependencyManager = new InferDependencyManager(environmentContext);
            boolean createFinished = createInferVirtualEnv(inferDependencyManager, environmentContext.getVirtualEnvDirectory());
            environmentContext.setFinished(createFinished);
            if (createFinished) {
                finishFile.createNewFile();
            } else {
                failedFile.createNewFile();
                throw new GeaflowRuntimeException("execute virtual env shell failed");
            }
        } catch (Throwable e) {
            ERROR_CASE.set(e);
            LOGGER.error("construct infer environment failed", e);
        } finally {
            if (lock != null) {
                releaseLock(lock);
            }
        }
        return environmentContext;
    }

    private boolean createInferVirtualEnv(InferDependencyManager dependencyManager, String workingDir) {
        String shellPath = dependencyManager.getBuildInferEnvShellPath();
        List<String> execParams = new ArrayList<>();
        String requirementsPath = dependencyManager.getInferEnvRequirementsPath();
        execParams.add(workingDir);
        execParams.add(requirementsPath);
        String conda = configuration.getString(INFER_ENV_CONDA_URL);
        execParams.add(conda);
        List<String> shellCommand = new ArrayList<>(Arrays.asList(SHELL_START, shellPath));
        shellCommand.addAll(execParams);
        String cmd = Joiner.on(" ").join(shellCommand);
        LOGGER.info("create infer virtual env {}", cmd);

        // Run "chmod 755 $shellPath"
        List<String> runCommands = new ArrayList<>();
        runCommands.add(CHMOD_CMD);
        runCommands.add(CHMOD_PERMISSION);
        runCommands.add(shellPath);
        String chmodCmd = Joiner.on(SCRIPT_SEPARATOR).join(runCommands);
        LOGGER.info("change {} permission run command is {}", shellPath, chmodCmd);
        int installEnvTimeOut = configuration.getInteger(FrameworkConfigKeys.INFER_ENV_INIT_TIMEOUT_SEC);
        if (!ShellExecUtils.run(chmodCmd, Duration.ofSeconds(installEnvTimeOut), LOGGER::info, LOGGER::error)) {
            return false;
        }
        return ShellExecUtils.run(cmd, Duration.ofSeconds(installEnvTimeOut), LOGGER::info, LOGGER::error, workingDir);
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    public static Boolean checkInferEnvironmentStatus() {
        return SUCCESS_FLAG.get();
    }

    public static InferEnvironmentContext getEnvironmentContext() {
        return environmentContext;
    }

    public static void checkError() {
        final Throwable exception = ERROR_CASE.get();
        if (exception != null) {
            String message = "create infer environment failed: " + exception.getMessage();
            LOGGER.error(message);
            throw new GeaflowRuntimeException(message, exception);
        }
    }

}
