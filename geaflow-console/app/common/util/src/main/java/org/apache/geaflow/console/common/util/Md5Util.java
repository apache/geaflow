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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.springframework.core.io.InputStreamSource;

public class Md5Util {

    public static String encodeString(String text) {
        if (text == null) {
            return null;
        }
        return DigestUtils.md5Hex(text);
    }

    public static String encodeFile(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "Invalid path");
        try (InputStream input = Files.newInputStream(new File(path).toPath())) {
            return DigestUtils.md5Hex(input);

        } catch (Exception e) {
            throw new GeaflowIllegalException("Encode md5 of file {} failed", path, e);
        }
    }

    public static String encodeFile(InputStreamSource stream) throws IOException {
        Preconditions.checkNotNull(stream, "Invalid stream source");
        try (InputStream input = stream.getInputStream()) {
            return DigestUtils.md5Hex(input);

        } catch (Exception e) {
            throw new GeaflowIllegalException("Encode stream source failed", e);
        }
    }

    public static String encodeFile(InputStream stream) throws IOException {
        Preconditions.checkNotNull(stream, "Invalid stream");
        try (InputStream input = stream) {
            return DigestUtils.md5Hex(input);

        } catch (Exception e) {
            throw new GeaflowIllegalException("Encode stream failed", e);
        }
    }
}
