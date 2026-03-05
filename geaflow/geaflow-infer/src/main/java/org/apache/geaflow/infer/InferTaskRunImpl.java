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

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_USER_DEFINE_LIB_PATH;
import static org.apache.geaflow.infer.InferTaskStatus.FAILED;

import com.google.common.base.Joiner;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.log.ProcessLoggerManager;
import org.apache.geaflow.infer.log.Slf4JProcessOutputConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferTaskRunImpl implements InferTaskRun {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferTaskRunImpl.class);

    private static final File NULL_FILE = new File((System.getProperty("os.name").startsWith(
        "Windows") ? "NUL" : "/dev/null"));

    private static final long TIMEOUT_SECOND = 10;
    private static final String SCRIPT_SEPARATOR = " ";
    private static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
    private static final String PATH = "PATH";
    private static final String PATH_REGEX = ":";
    private static final String PYTHON_PATH = "PYTHONPATH";
    private final InferEnvironmentContext inferEnvironmentContext;
    private final Configuration jobConfig;
    private final String virtualEnvPath;
    private final String inferFilePath;
    private final String executePath;
    private Process inferTask;
    private String inferScript;

    private InferTaskStatus inferTaskStatus;

    public InferTaskRunImpl(InferEnvironmentContext inferEnvironmentContext) {
        this.inferEnvironmentContext = inferEnvironmentContext;
        this.jobConfig = inferEnvironmentContext.getJobConfig();
        this.inferFilePath = inferEnvironmentContext.getInferFilesDirectory();
        this.virtualEnvPath = inferEnvironmentContext.getVirtualEnvDirectory();
        this.executePath = this.virtualEnvPath + "/bin";
    }

    @Override
    public void run(List<String> script) {
        inferScript = Joiner.on(SCRIPT_SEPARATOR).join(script);
        LOGGER.info("infer task run command is {}", inferScript);
        ProcessBuilder inferTaskBuilder = new ProcessBuilder(script);
        buildInferTaskBuilder(inferTaskBuilder);
        try {
            inferTask = inferTaskBuilder.start();
            this.inferTaskStatus = InferTaskStatus.RUNNING;
            try (ProcessLoggerManager processLogger = new ProcessLoggerManager(inferTask, new Slf4JProcessOutputConsumer(this.getClass().getSimpleName()))) {
                processLogger.startLogging();
                int exitValue = 0;
                if (inferTask.waitFor(TIMEOUT_SECOND, TimeUnit.SECONDS)) {
                    exitValue = inferTask.exitValue();
                    this.inferTaskStatus = FAILED;
                } else {
                    this.inferTaskStatus = InferTaskStatus.RUNNING;
                }
                if (exitValue != 0) {
                    throw new GeaflowRuntimeException(
                        String.format("infer task [%s] run failed, exitCode is %d, message is "
                                      + "%s", inferScript, exitValue, processLogger.getErrorOutputLogger().get()));
                }
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException("infer task run failed", e);
        } finally {
            if (inferTask != null && inferTaskStatus.equals(FAILED)) {
                inferTask.destroyForcibly();
            }
        }
    }

    @Override
    public void stop() {
        if (inferTask != null) {
            inferTask.destroyForcibly();
        }
    }

    private void buildInferTaskBuilder(ProcessBuilder processBuilder) {
        Map<String, String> environment = processBuilder.environment();
        environment.put(PATH, executePath);
        processBuilder.directory(new File(this.inferFilePath));
        processBuilder.redirectErrorStream(true);
        setLibraryPath(processBuilder);
        environment.computeIfAbsent(PYTHON_PATH, k -> virtualEnvPath);
        processBuilder.redirectOutput(NULL_FILE);
    }


    private void setLibraryPath(ProcessBuilder processBuilder) {
        List<String> userDefineLibPath = getUserDefineLibPath();
        StringBuilder libBuilder = new StringBuilder();
        libBuilder.append(this.inferEnvironmentContext.getInferLibPath());
        libBuilder.append(PATH_REGEX);
        for (String ldLibraryPath : userDefineLibPath) {
            libBuilder.append(ldLibraryPath);
            libBuilder.append(PATH_REGEX);
        }
        String ldLibraryPathEnvVar = System.getenv(LD_LIBRARY_PATH);
        libBuilder.append(ldLibraryPathEnvVar);
        processBuilder.environment().put(LD_LIBRARY_PATH, libBuilder.toString());
    }

    private List<String> getUserDefineLibPath() {
        String userLibPath = jobConfig.getString(INFER_USER_DEFINE_LIB_PATH.getKey());
        List<String> result = new ArrayList<>();
        if (userLibPath != null) {
            String[] libs = userLibPath.split(",");
            Iterator<String> iterator = Arrays.stream(libs).iterator();
            while (iterator.hasNext()) {
                String libPath = this.inferFilePath + File.separator + iterator.next().trim();
                LOGGER.info("define infer lib path is {}", libPath);
                result.add(libPath);
            }
        }
        return result;
    }
}
