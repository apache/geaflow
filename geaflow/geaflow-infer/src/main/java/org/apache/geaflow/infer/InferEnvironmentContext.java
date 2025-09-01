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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class InferEnvironmentContext {

    private static final String PROCESS_ID_FLAG = "@";

    private static final String HOST_SEPARATOR = ":";

    private static final String LIB_PATH = "/conda/lib";

    private static final String INFER_SCRIPT_FILE = "/infer_server.py";

    private static final String PYTHON_EXEC = "/conda/bin/python3";

    // Start infer process parameter.
    private static final String TF_CLASSNAME_KEY = "--tfClassName=";

    private static final String SHARE_MEMORY_INPUT_KEY = "--input_queue_shm_id=";

    private static final String SHARE_MEMORY_OUTPUT_KEY = "--output_queue_shm_id=";

    private final String virtualEnvDirectory;

    private final String inferFilesDirectory;

    private final String inferLibPath;

    private Boolean envFinished;

    private final String roleNameIndex;

    private final Configuration configuration;

    private String inferScript;

    private String pythonExec;


    public InferEnvironmentContext(String virtualEnvDirectory, String pythonFilesDirectory,
                                   Configuration configuration) {
        this.virtualEnvDirectory = virtualEnvDirectory;
        this.inferFilesDirectory = pythonFilesDirectory;
        this.inferLibPath = virtualEnvDirectory + LIB_PATH;
        this.pythonExec = virtualEnvDirectory + PYTHON_EXEC;
        this.inferScript = pythonFilesDirectory + INFER_SCRIPT_FILE;
        this.roleNameIndex = queryRoleNameIndex();
        this.configuration = configuration;
        this.envFinished = false;
    }

    private String queryRoleNameIndex() {
        try {
            InetAddress address = InetAddress.getLocalHost();
            String hostName = address.getHostName();
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String name = runtime.getName();
            int processId = Integer.parseInt(name.substring(0, name.indexOf(PROCESS_ID_FLAG)));
            return hostName + HOST_SEPARATOR + processId;
        } catch (Exception e) {
            throw new GeaflowRuntimeException("query role name and index failed", e);
        }
    }

    public String getVirtualEnvDirectory() {
        return virtualEnvDirectory;
    }

    public String getInferFilesDirectory() {
        return inferFilesDirectory;
    }

    public Boolean enableFinished() {
        return envFinished;
    }

    public void setPythonExec(String pythonExec) {
        this.pythonExec = pythonExec;
    }

    public void setInferScript(String inferScript) {
        this.inferScript = inferScript;
    }

    public void setFinished(Boolean envFinished) {
        this.envFinished = envFinished;
    }

    public String getRoleNameIndex() {
        return roleNameIndex;
    }

    public String getInferLibPath() {
        return inferLibPath;
    }

    public String getPythonExec() {
        return pythonExec;
    }

    public Configuration getJobConfig() {
        return configuration;
    }

    public String getInferTFClassNameParam(String udfClassName) {
        return TF_CLASSNAME_KEY + udfClassName;
    }

    public String getInferShareMemoryInputParam(String shareMemoryInputKey) {
        return SHARE_MEMORY_INPUT_KEY + shareMemoryInputKey;
    }

    public String getInferShareMemoryOutputParam(String shareMemoryOutputKey) {
        return SHARE_MEMORY_OUTPUT_KEY + shareMemoryOutputKey;
    }

    public String getInferScript() {
        return inferScript;
    }
}
