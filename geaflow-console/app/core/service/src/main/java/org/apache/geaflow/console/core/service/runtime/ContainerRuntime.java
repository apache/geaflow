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

package org.apache.geaflow.console.core.service.runtime;

import com.alibaba.fastjson.JSON;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.FileUtil;
import org.apache.geaflow.console.common.util.ProcessUtil;
import org.apache.geaflow.console.common.util.RetryUtil;
import org.apache.geaflow.console.common.util.ZipUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowLogException;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.model.job.config.GeaflowArgsClass;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.task.ContainerTaskHandle;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.model.task.GeaflowTaskHandle;
import org.apache.geaflow.console.core.service.InstanceService;
import org.apache.geaflow.console.core.service.file.LocalFileFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ContainerRuntime implements GeaflowRuntime {

    private static final String GEAFLOW_ENGINE_LOG_FILE = "/tmp/logs/task/%s.log";

    private static final String GEAFLOW_ENGINE_FINISH_FILE = "/tmp/logs/task/%s.finish";

    private static final String GEAFLOW_LOG4J_PROPERTIES = "log4j.properties";

    @Autowired
    private ContainerTaskParams taskParams;

    @Autowired
    protected LocalFileFactory localFileFactory;

    @Autowired
    private InstanceService instanceService;

    public static String getLogFilePath(String taskId) {
        return String.format(GEAFLOW_ENGINE_LOG_FILE, TaskParams.getRuntimeTaskName(taskId));
    }

    public static String getFinishFilePath(String taskId) {
        return String.format(GEAFLOW_ENGINE_FINISH_FILE, TaskParams.getRuntimeTaskName(taskId));
    }

    @Override
    public GeaflowTaskHandle start(GeaflowTask task) {
        GeaflowInstance instance = instanceService.get(task.getRelease().getJob().getInstanceId());
        return doStart(task, taskParams.buildClientArgs(instance, task));
    }

    @Override
    public void stop(GeaflowTask task) {
        try {
            int pid = ((ContainerTaskHandle) task.getHandle()).getPid();
            if (ProcessUtil.existPid(pid)) {
                ProcessUtil.killPid(pid);
            }

        } catch (Exception e) {
            throw new GeaflowLogException("Stop task {} failed", task.getId(), e);
        }
    }

    @Override
    public GeaflowTaskStatus queryStatus(GeaflowTask task) {
        try {
            return RetryUtil.exec(() -> {
                int pid = ((ContainerTaskHandle) task.getHandle()).getPid();
                if (ProcessUtil.existPid(pid)) {
                    return GeaflowTaskStatus.RUNNING;
                }

                if (FileUtil.exist(getFinishFilePath(task.getId()))) {
                    return GeaflowTaskStatus.FINISHED;
                }

                return GeaflowTaskStatus.FAILED;
            }, 5, 500);

        } catch (Exception e) {
            log.error("Query task {} status failed, handle={}", task.getId(), JSON.toJSONString(task.getHandle()), e);
            return GeaflowTaskStatus.FAILED;
        }
    }

    private GeaflowTaskHandle doStart(GeaflowTask task, GeaflowArgsClass geaflowArgs) {
        String runtimeTaskId = geaflowArgs.getSystemArgs().getRuntimeTaskId();
        taskParams.validateRuntimeTaskId(runtimeTaskId);

        try {
            // kill last task process if exists
            GeaflowTaskHandle handle = task.getHandle();
            if (handle != null) {
                int pid = ((ContainerTaskHandle) handle).getPid();
                if (ProcessUtil.existPid(pid)) {
                    ProcessUtil.killPid(pid);
                }
            }

            // clear finish file if exists
            String finishFile = getFinishFilePath(task.getId());
            FileUtil.delete(finishFile);

            List<String> classPaths = new ArrayList<>();

            // add version jar
            GeaflowRelease release = task.getRelease();
            String versionName = release.getVersion().getName();
            task.getVersionJars()
                .forEach(jar -> classPaths.add(localFileFactory.getVersionFile(versionName, jar).getAbsolutePath()));

            // add user jar
            task.getUserJars()
                .forEach(jar -> classPaths.add(localFileFactory.getTaskUserFile(runtimeTaskId, jar).getAbsolutePath()));

            // add release zip
            File releaseFile = localFileFactory.getTaskReleaseFile(runtimeTaskId, release.getJob().getId(), release);
            ZipUtil.unzip(releaseFile);
            classPaths.add(releaseFile.getParent());

            // start task process
            String java = System.getProperty("java.home") + "/bin/java";
            String classPathString = StringUtils.join(classPaths, ":");
            String mainClass = task.getMainClass();
            String args = StringEscapeUtils.escapeJava(JSON.toJSONString(geaflowArgs.build()));
            String logFilePath = getLogFilePath(task.getId());
            CommandLine cmd = new CommandLine(java);
            cmd.addArgument("-cp");
            cmd.addArgument(classPathString);
            cmd.addArgument("-Dlog.file=" + logFilePath);
            cmd.addArgument("-Dlog4j.configuration=" + GEAFLOW_LOG4J_PROPERTIES);
            cmd.addArgument(mainClass);
            cmd.addArgument(args, false);
            int pid = ProcessUtil.execAsyncCommand(cmd, 1000, logFilePath, finishFile);

            // save handle
            ContainerTaskHandle taskHandle = new ContainerTaskHandle(runtimeTaskId, pid);
            log.info("Start task {} success, handle={}", task.getId(), JSON.toJSONString(taskHandle));
            return taskHandle;

        } catch (Exception e) {
            throw new GeaflowLogException("Start task {} failed", task.getId(), e);
        }
    }

}
