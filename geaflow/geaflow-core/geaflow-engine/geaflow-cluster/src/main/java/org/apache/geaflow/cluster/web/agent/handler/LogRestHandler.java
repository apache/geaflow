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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.LOG_DIR;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.web.agent.model.FileInfo;
import org.apache.geaflow.cluster.web.agent.model.PaginationRequest;
import org.apache.geaflow.cluster.web.agent.model.PaginationResponse;
import org.apache.geaflow.cluster.web.agent.util.FileUtil;
import org.apache.geaflow.cluster.web.api.ApiResponse;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/logs")
public class LogRestHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogRestHandler.class);

    private final String runtimeLogDirPath;

    private final Pattern logPattern;

    public LogRestHandler(String runtimeLogDirPath) {
        this.runtimeLogDirPath = runtimeLogDirPath;
        this.logPattern = Pattern.compile(String.format("%s.*\\.log(\\.\\d*)?", this.runtimeLogDirPath));
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<List<FileInfo>> getLogList() {
        try {
            checkRuntimeLogDirPath();
            List<FileInfo> logs = new ArrayList<>();
            File file = new File(runtimeLogDirPath);
            String[] fileList = file.list();
            for (String f : fileList) {
                String logPath = runtimeLogDirPath + File.separator + f;
                File logFile = new File(logPath);
                if (logFile.isFile() && logPattern.matcher(logPath).matches()) {
                    FileInfo fileInfo = FileUtil.buildFileInfo(logFile, logPath);
                    logs.add(fileInfo);
                }
            }
            return ApiResponse.success(logs);
        } catch (Throwable t) {
            LOGGER.error("Query log file list failed. {}", t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    @GET
    @Path("/content")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResponse<PaginationResponse<String>> getLogContent(@QueryParam("path") String logPath,
                                                                 @QueryParam("pageNo") int pageNo,
                                                                 @QueryParam("pageSize") int pageSize) {
        try {
            checkLogPath(logPath);
            PaginationRequest request = new PaginationRequest(pageNo, pageSize);
            FileUtil.checkPaginationRequest(request);
            PaginationResponse<String> response = FileUtil.getFileContent(request, logPath);
            if (response == null) {
                throw new GeaflowRuntimeException(
                    String.format("Log file %s not exists.", logPath));
            }
            return ApiResponse.success(response);
        } catch (Throwable t) {
            LOGGER.error("Query log content {} failed. {}", logPath, t.getMessage(), t);
            return ApiResponse.error(t);
        }
    }

    private void checkRuntimeLogDirPath() {
        if (StringUtils.isEmpty(runtimeLogDirPath)) {
            throw new GeaflowRuntimeException(String.format("Log dir path is not set. Please set the log "
                + "dir path config: %s", LOG_DIR.getKey()));
        }
    }

    private void checkLogPath(String logPath) {
        if (logPath == null || !logPattern.matcher(logPath).matches()) {
            throw new GeaflowRuntimeException(String.format("Log path %s is invalid.", logPath));
        }
    }
}
