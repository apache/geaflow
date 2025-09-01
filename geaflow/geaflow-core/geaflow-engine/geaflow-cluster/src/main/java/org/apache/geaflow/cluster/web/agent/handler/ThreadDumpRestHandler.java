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

import com.google.common.base.Preconditions;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.geaflow.cluster.web.agent.model.PaginationRequest;
import org.apache.geaflow.cluster.web.agent.model.PaginationResponse;
import org.apache.geaflow.cluster.web.agent.model.ThreadDumpRequest;
import org.apache.geaflow.cluster.web.agent.model.ThreadDumpResponse;
import org.apache.geaflow.cluster.web.agent.util.FileUtil;
import org.apache.geaflow.cluster.web.api.ApiResponse;
import org.apache.geaflow.common.utils.ShellUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/thread-dump")
public class ThreadDumpRestHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadDumpRestHandler.class);

    private static final String THREAD_DUMP_FILE_NAME = "geaflow-thread-dump.log";

    private final String threadDumpFilePath;

    public ThreadDumpRestHandler(String agentDir) {
        this.threadDumpFilePath = Paths.get(agentDir, THREAD_DUMP_FILE_NAME).toString();
    }

    @GET
    @Path("/content")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<PaginationResponse<ThreadDumpResponse>> getThreadDumpFileContent(@QueryParam("pageNo") int pageNo,
                                                                                        @QueryParam("pageSize") int pageSize) {
        try {
            PaginationRequest request = new PaginationRequest(pageNo, pageSize);
            FileUtil.checkPaginationRequest(request);
            PaginationResponse<String> response = FileUtil.getFileContent(request, threadDumpFilePath);
            if (response == null) {
                LOGGER.warn("Thread-dump log file {} not exists.", threadDumpFilePath);
                return ApiResponse.success(new PaginationResponse<>(0, null));
            }
            File file = new File(threadDumpFilePath);
            ThreadDumpResponse threadDumpResponse = new ThreadDumpResponse();
            threadDumpResponse.setLastDumpTime(file.lastModified());
            threadDumpResponse.setContent(response.getData());
            return ApiResponse.success(new PaginationResponse<>(response.getTotal(), threadDumpResponse));
        } catch (Throwable t) {
            LOGGER.error("Query thread-dump log content failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public ApiResponse<Void> executeThreadDumpProfiler(ThreadDumpRequest request) {
        try {
            checkThreadDumpRequest(request);
            ProcessBuilder command = getCommand(request.getPid());
            ShellUtil.executeShellCommand(command, 30);
            return ApiResponse.success();
        } catch (Throwable t) {
            LOGGER.error("Execute thread-dump command failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    private void checkThreadDumpRequest(ThreadDumpRequest request) {
        Preconditions.checkArgument(request.getPid() > 0, "Pid must be larger than 0.");
    }

    private ProcessBuilder getCommand(int pid) {
        List<String> commands = new ArrayList<>();
        commands.add("sh");
        commands.add("-c");
        commands.add(String.format("jstack -l %d > %s", pid, threadDumpFilePath));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
        return processBuilder;
    }

}
