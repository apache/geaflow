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
        // First compile Cython modules (if setup.py exists)
        compileCythonModules();
        
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

    /**
     * Compile Cython modules if setup.py exists.
     * This is required for modules like mmap_ipc that need compilation.
     */
    private void compileCythonModules() {
        File setupPy = new File(inferFilePath, "setup.py");
        if (!setupPy.exists()) {
            LOGGER.debug("setup.py not found, skipping Cython compilation");
            return;
        }
        
        try {
            String pythonExec = inferEnvironmentContext.getPythonExec();
            
            // 1. 首先尝试安装 Cython（如果还没安装）
            ensureCythonInstalled(pythonExec);
            
            // 2. 清理旧的编译产物（.cpp, .so 等）以避免冲突
            cleanOldCompiledFiles();
            
            // 3. 然后编译 Cython 模块
            List<String> compileCythonCmd = new ArrayList<>();
            compileCythonCmd.add(pythonExec);
            compileCythonCmd.add("setup.py");
            compileCythonCmd.add("build_ext");
            compileCythonCmd.add("--inplace");
            
            LOGGER.info("Compiling Cython modules: {}", String.join(" ", compileCythonCmd));
            
            ProcessBuilder cythonBuilder = new ProcessBuilder(compileCythonCmd);
            cythonBuilder.directory(new File(inferFilePath));
            cythonBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
            cythonBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
            
            Process cythonProcess = cythonBuilder.start();
            ProcessLoggerManager processLogger = new ProcessLoggerManager(cythonProcess, 
                new Slf4JProcessOutputConsumer("CythonCompiler"));
            processLogger.startLogging();
            
            boolean finished = cythonProcess.waitFor(60, TimeUnit.SECONDS);
            
            if (finished) {
                int exitCode = cythonProcess.exitValue();
                if (exitCode == 0) {
                    LOGGER.info("✓ Cython modules compiled successfully");
                } else {
                    String errorMsg = processLogger.getErrorOutputLogger().get();
                    LOGGER.error("✗ Cython compilation failed with exit code: {}. Error: {}", 
                        exitCode, errorMsg);
                    throw new GeaflowRuntimeException(
                        String.format("Cython compilation failed (exit code %d): %s", exitCode, errorMsg));
                }
            } else {
                LOGGER.error("✗ Cython compilation timed out after 60 seconds");
                cythonProcess.destroyForcibly();
                throw new GeaflowRuntimeException("Cython compilation timed out");
            }
        } catch (GeaflowRuntimeException e) {
            throw e;
        } catch (Exception e) {
            String errorMsg = String.format("Cython compilation failed: %s", e.getMessage());
            LOGGER.error(errorMsg, e);
            throw new GeaflowRuntimeException(errorMsg, e);
        }
    }

    /**
     * Clean up old compiled files (.cpp, .c, .so, .pyd) to avoid Cython compilation conflicts.
     */
    private void cleanOldCompiledFiles() {
        try {
            File inferDir = new File(inferFilePath);
            if (!inferDir.exists() || !inferDir.isDirectory()) {
                return;
            }
            
            String[] extensions = {".cpp", ".c", ".so", ".pyd", ".o"};
            File[] files = inferDir.listFiles((dir, name) -> {
                for (String ext : extensions) {
                    if (name.endsWith(ext)) {
                        return true;
                    }
                }
                return false;
            });
            
            if (files != null) {
                for (File file : files) {
                    boolean deleted = file.delete();
                    if (deleted) {
                        LOGGER.debug("Cleaned old compiled file: {}", file.getName());
                    } else {
                        LOGGER.warn("Failed to delete old compiled file: {}", file.getName());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to clean old compiled files: {}", e.getMessage());
        }
    }

    /**
     * Ensure Cython is installed in the Python environment.
     * Attempts to import it, and if not found, installs it via pip.
     */
    private void ensureCythonInstalled(String pythonExec) {
        try {
            // 1. Check if Cython is already installed
            List<String> checkCmd = new ArrayList<>();
            checkCmd.add(pythonExec);
            checkCmd.add("-c");
            checkCmd.add("from Cython.Build import cythonize; print('Cython is already installed')");
            
            ProcessBuilder checkBuilder = new ProcessBuilder(checkCmd);
            Process checkProcess = checkBuilder.start();
            boolean checkFinished = checkProcess.waitFor(10, TimeUnit.SECONDS);
            
            if (checkFinished && checkProcess.exitValue() == 0) {
                LOGGER.info("✓ Cython is already installed");
                return;  // Cython 已安装，无需再安装
            }
            
            // 2. Cython not found, try to install via pip
            LOGGER.info("Cython not found, attempting to install via pip...");
            List<String> installCmd = new ArrayList<>();
            installCmd.add(pythonExec);
            installCmd.add("-m");
            installCmd.add("pip");
            installCmd.add("install");
            installCmd.add("--user");
            installCmd.add("Cython>=0.29.0");
            
            ProcessBuilder installBuilder = new ProcessBuilder(installCmd);
            Process installProcess = installBuilder.start();
            ProcessLoggerManager processLogger = new ProcessLoggerManager(installProcess, 
                new Slf4JProcessOutputConsumer("CythonInstaller"));
            processLogger.startLogging();
            
            boolean finished = installProcess.waitFor(120, TimeUnit.SECONDS);
            
            if (finished && installProcess.exitValue() == 0) {
                LOGGER.info("✓ Cython installed successfully");
            } else {
                String errorMsg = processLogger.getErrorOutputLogger().get();
                LOGGER.warn("Failed to install Cython via pip: {}", errorMsg);
                throw new GeaflowRuntimeException(
                    String.format("Failed to install Cython: %s", errorMsg));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GeaflowRuntimeException("Cython installation interrupted", e);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(
                String.format("Failed to ensure Cython installation: %s", e.getMessage()), e);
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
        //  保留 stderr 用于调试，但忽略 stdout
        processBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
        processBuilder.redirectOutput(NULL_FILE);
        setLibraryPath(processBuilder);
        environment.computeIfAbsent(PYTHON_PATH, k -> virtualEnvPath);
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
