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

package org.apache.geaflow.mcp.server.util;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2025 All Rights Reserved.
 *
 * @author lt on 2025/9/1.
 */
public class McpLocalFileUtil {


    public static String createAndWriteFile(String root, String text, String... fileNames) throws IOException {
        // 生成文件名
        Files.createDirectories(Paths.get(root));
        String fileName = "execute_query_" + Instant.now().toEpochMilli();
        if (fileNames != null && fileNames.length > 0) {
            fileName = fileNames[0];
        }

        // 构建完整路径
        String fullPath = Paths.get(root, fileName).toString();

        // 创建文件并写入内容
        try (FileWriter writer = new FileWriter(fullPath)) {
            if (text != null) {
                writer.write(text);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileName;
    }

    public static String readFile(String root, String fileName) throws IOException {
        // 构建完整文件路径
        Path filePath = Paths.get(root, fileName);

        // 检查文件是否存在
        if (!Files.exists(filePath)) {
            throw new IOException("文件不存在: " + filePath);
        }

        // 检查是否为普通文件
        if (!Files.isRegularFile(filePath)) {
            throw new IOException("路径不是文件: " + filePath);
        }

        // 读取文件内容并返回
        return new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
    }
}
