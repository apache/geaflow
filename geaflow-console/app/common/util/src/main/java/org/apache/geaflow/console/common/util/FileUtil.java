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

package org.apache.geaflow.console.common.util;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

public class FileUtil {

    public static boolean exist(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        return new File(path).exists();
    }

    public static void touch(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        try {
            FileUtils.touch(new File(path));

        } catch (Exception e) {
            throw new GeaflowException("Create file {} failed", path, e);
        }
    }

    public static void mkdir(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        try {
            FileUtils.forceMkdir(new File(path));

        } catch (Exception e) {
            throw new GeaflowException("Create directory {} failed", path, e);
        }
    }

    public static boolean delete(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        return FileUtils.deleteQuietly(new File(path));
    }

    public static String readFileContent(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        try {
            return FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8.name());

        } catch (Exception e) {
            throw new GeaflowException("Read file {} failed", path, e);
        }
    }

    public static InputStream readFileStream(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        try {
            return FileUtils.openInputStream(new File(path));

        } catch (Exception e) {
            throw new GeaflowException("Read file {} failed", path, e);
        }
    }

    public static void writeFile(String path, String content) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        try {
            FileUtils.write(new File(path), content, StandardCharsets.UTF_8);

        } catch (Exception e) {
            throw new GeaflowException("Write file {} failed", path, e);
        }
    }

    public static void writeFile(String path, InputStream stream) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        File file = new File(path);

        try (InputStream in = stream; OutputStream out = FileUtils.openOutputStream(file)) {
            IOUtils.copy(in, out, 1024 * 1024 * 8);

        } catch (Exception e) {
            throw new GeaflowException("Write file {} failed", path, e);
        }
    }
}
