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

package org.apache.geaflow.cluster.web.agent.handler;

import static org.apache.geaflow.cluster.constants.ClusterConstants.AGENT_PROFILER_PATH;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.web.agent.model.FileInfo;
import org.apache.geaflow.cluster.web.agent.model.FlameGraphRequest;
import org.apache.geaflow.cluster.web.agent.model.FlameGraphType;
import org.apache.geaflow.cluster.web.agent.util.DateUtil;
import org.apache.geaflow.cluster.web.agent.util.FileUtil;
import org.apache.geaflow.cluster.web.api.ApiResponse;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.ShellUtil;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/flame-graphs")
public class FlameGraphRestHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlameGraphRestHandler.class);

    private static final String FLAME_GRAPH_FILE_PREFIX = "geaflow-flamegraph";

    private static final int FLAME_GRAPH_FILE_MAX_CNT = 10;

    private final ExecutorService profileService = new ThreadPoolExecutor(1, 10, 30,
        TimeUnit.SECONDS, new LinkedBlockingQueue<>(10),
        ThreadUtil.namedThreadFactory(true, "flame-graph-profiler"));

    private final String flameGraphProfilerPath;

    private final String flameGraphFileNameExtension;

    private final String agentDir;

    public FlameGraphRestHandler(String flameGraphProfilerPath, String flameGraphFileNameExtension, String agentDir) {
        this.flameGraphProfilerPath = flameGraphProfilerPath;
        this.flameGraphFileNameExtension = flameGraphFileNameExtension;
        this.agentDir = agentDir;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<List<FileInfo>> getFlameGraphFileList() {
        try {
            List<FileInfo> flameGraphFiles = new ArrayList<>();
            File file = new File(agentDir);
            String[] fileList = file.list();
            if (fileList != null) {
                for (String f : fileList) {
                    String filePath = agentDir + File.separator + f;
                    File flameGraphFile = new File(filePath);
                    if (flameGraphFile.isFile() && f.startsWith(FLAME_GRAPH_FILE_PREFIX)
                        && f.endsWith(flameGraphFileNameExtension)) {
                        FileInfo fileInfo = FileUtil.buildFileInfo(flameGraphFile, filePath);
                        flameGraphFiles.add(fileInfo);
                    }
                }
            }
            return ApiResponse.success(flameGraphFiles);
        } catch (Throwable t) {
            LOGGER.error("Query flame-graph file list failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/content")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<String> getFlameGraphFileContent(@QueryParam("path") String filePath) {
        try {
            checkFlameGraphFilePath(filePath);
            String content = org.apache.geaflow.common.utils.FileUtil.getContentFromFile(filePath);
            if (content == null) {
                throw new GeaflowRuntimeException(
                    String.format("Flame-graph file %s not exists.", filePath));
            }
            return ApiResponse.success(content);
        } catch (Throwable t) {
            LOGGER.error("Query flame-graph file content failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public ApiResponse<Void> executeFlameGraphProfiler(FlameGraphRequest request) {
        try {
            checkProfilerPath();
            checkFlameGraphRequest(request);
            checkFlameGraphFileCount();
            ProcessBuilder command = getCommand(request);
            profileService.submit(() -> ShellUtil.executeShellCommand(command, 90));
            return ApiResponse.success();
        } catch (Throwable t) {
            LOGGER.error("Execute flame-graph profiler command failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @DELETE
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<Void> deleteFlameGraphFile(@QueryParam("path") String filePath) {
        try {
            checkFlameGraphFilePath(filePath);
            File file = new File(filePath);
            if (!file.exists() || !file.isFile()) {
                throw new GeaflowRuntimeException(String.format("File %s not found.", filePath));
            }
            file.delete();
            return ApiResponse.success();
        } catch (Throwable t) {
            LOGGER.error("Delete flame-graph file failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    private void checkFlameGraphFileCount() {
        File file = new File(agentDir);
        String[] fileList = file.list();
        int cnt = 0;
        if (fileList != null) {
            for (String f : fileList) {
                String filePath = agentDir + File.separator + f;
                File flameGraphFile = new File(filePath);
                if (flameGraphFile.isFile() && f.startsWith(FLAME_GRAPH_FILE_PREFIX) && f.endsWith(
                    flameGraphFileNameExtension)) {
                    cnt++;
                }
            }
        }
        if (cnt >= FLAME_GRAPH_FILE_MAX_CNT) {
            throw new GeaflowRuntimeException(String.format(
                "The count of flame-graph files is " + "limited to " + "%s. "
                    + "Please delete some of them first.", FLAME_GRAPH_FILE_MAX_CNT));
        }
    }

    private void checkProfilerPath() {
        if (StringUtils.isEmpty(flameGraphProfilerPath)) {
            throw new GeaflowRuntimeException(String.format("Async-profiler shell script path is "
                    + "not set. Please set the file path of async-profiler path: %s",
                AGENT_PROFILER_PATH));
        }
    }

    private void checkFlameGraphFilePath(String path) {
        Preconditions.checkArgument(
            path != null && path.startsWith(agentDir) && path.endsWith(
                flameGraphFileNameExtension), "File path is invalid.");
    }

    private void checkFlameGraphRequest(FlameGraphRequest request) {
        Preconditions.checkArgument(request.getType() != null, "Profiler type cannot be null.");
        Preconditions.checkArgument(request.getDuration() > 0 && request.getDuration() <= 60,
            "Duration must be within 0~60 seconds.");
        Preconditions.checkArgument(request.getPid() > 0, "Pid must be larger than 0.");
    }

    private ProcessBuilder getCommand(FlameGraphRequest request) {
        String now = DateUtil.simpleFormat(System.currentTimeMillis());
        String randomSuffix = RandomStringUtils.randomAlphabetic(4);
        StringBuilder filePath = new StringBuilder();
        filePath.append(agentDir).append("/").append(FLAME_GRAPH_FILE_PREFIX).append("-")
            .append("pid").append(request.getPid()).append("-").append(request.getType())
            .append("-").append(request.getDuration()).append("s").append("-").append(now)
            .append("-").append(randomSuffix).append(flameGraphFileNameExtension);
        List<String> commands = new ArrayList<>();
        commands.add("sh");
        commands.add(flameGraphProfilerPath);
        commands.add("--all-user");
        commands.add("-d");
        commands.add(String.valueOf(request.getDuration()));
        if (request.getType() == FlameGraphType.ALLOC) {
            commands.add("-e");
            commands.add(FlameGraphType.ALLOC.name().toLowerCase());
        }
        commands.add("-f");
        commands.add(filePath.toString());
        commands.add(String.valueOf(request.getPid()));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
        return processBuilder;
    }

}
