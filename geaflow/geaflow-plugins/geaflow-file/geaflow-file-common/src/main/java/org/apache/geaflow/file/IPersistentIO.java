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

package org.apache.geaflow.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public interface IPersistentIO {

    void init(Configuration userConfig);

    List<String> listFileName(Path path) throws IOException;

    boolean exists(Path path) throws IOException;

    boolean delete(Path path, boolean recursive) throws IOException;

    boolean renameFile(Path from, Path to) throws IOException;

    boolean createNewFile(Path path) throws IOException;

    void copyFromLocalFile(Path local, Path remote) throws IOException;

    void copyToLocalFile(Path remote, Path local) throws IOException;

    long getFileSize(Path path) throws IOException;

    long getFileCount(Path path) throws IOException;

    FileInfo getFileInfo(Path path) throws IOException;

    FileInfo[] listFileInfo(Path path) throws IOException;

    FileInfo[] listFileInfo(Path path, PathFilter filter) throws IOException;

    InputStream open(Path path) throws IOException;

    void close() throws IOException;

    PersistentType getPersistentType();
}
