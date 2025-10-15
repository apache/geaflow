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

package org.apache.geaflow.cluster.web.agent.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import org.apache.geaflow.cluster.web.agent.model.FileInfo;
import org.apache.geaflow.cluster.web.agent.model.PaginationRequest;
import org.apache.geaflow.cluster.web.agent.model.PaginationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtil.class);

    private static final String NUL_CHAR = "\\u0000";

    public static PaginationResponse<String> getFileContent(PaginationRequest request,
                                                            String filePath) {
        int start = (request.getPageNo() - 1) * request.getPageSize();
        File file = new File(filePath);
        if (file.exists()) {
            byte[] buf = new byte[request.getPageSize()];
            try (FileInputStream inputStream = new FileInputStream(file)) {
                inputStream.skip(start);
                inputStream.read(buf);
            } catch (IOException e) {
                throw new RuntimeException("Error read file content.", e);
            }
            PaginationResponse<String> response = new PaginationResponse<>();
            response.setData(new String(buf).replaceAll(NUL_CHAR, ""));
            response.setTotal(file.length());
            return response;
        }
        return null;
    }

    public static FileInfo buildFileInfo(File file, String path) {
        FileInfo fileInfo = new FileInfo();
        fileInfo.setPath(path);
        fileInfo.setSize(file.length());
        try {
            BasicFileAttributeView basicFileAttributeView =
                Files.getFileAttributeView(Paths.get(path),
                    BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            fileInfo.setCreatedTime(basicFileAttributeView.readAttributes().creationTime().toMillis());
        } catch (IOException e) {
            LOGGER.error("Get created time of file {} failed. {}", path, e.getMessage(), e);
        }
        return fileInfo;
    }

    public static void checkPaginationRequest(PaginationRequest request) {
        Preconditions.checkArgument(request.getPageNo() > 0,
            "Page number should be greater than 0.");
        Preconditions.checkArgument(request.getPageSize() > 0,
            "Page size should be greater than 0.");
    }

}
