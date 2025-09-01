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

package org.apache.geaflow.dsl.connector.file.sink;

import static org.apache.geaflow.dsl.connector.file.FileConstants.PREFIX_S3_RESOURCE;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

public class FileWriteHandlers {

    public static FileWriteHandler from(String path, Configuration conf) {
        if (path.startsWith(PREFIX_S3_RESOURCE)) {
            return new S3FileWriteHandler(path);
        }
        FileSystem fs = FileConnectorUtil.getHdfsFileSystem(conf);
        if (fs instanceof LocalFileSystem) {
            return new LocalFileWriteHandler(path);
        }
        return new HdfsFileWriteHandler(path);
    }
}
