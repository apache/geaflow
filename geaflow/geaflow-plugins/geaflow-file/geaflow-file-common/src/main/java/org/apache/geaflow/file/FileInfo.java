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

import java.util.Objects;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class FileInfo {

    private Path path;
    private long modifiedTime;
    private long length;

    protected FileInfo() {
    }

    public static FileInfo of(FileStatus fileStatus) {
        return new FileInfo()
            .withPath(fileStatus.getPath())
            .withLength(fileStatus.getLen())
            .withModifiedTime(fileStatus.getModificationTime());
    }

    public static FileInfo of() {
        return new FileInfo();
    }

    public FileInfo withLength(long length) {
        this.length = length;
        return this;
    }

    public FileInfo withPath(Path path) {
        this.path = path;
        return this;
    }

    public FileInfo withModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
        return this;
    }

    public Path getPath() {
        return path;
    }

    public long getModificationTime() {
        return modifiedTime;
    }

    public long getLength() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileInfo fileInfo = (FileInfo) o;
        return length == fileInfo.getLength()
            && modifiedTime == fileInfo.modifiedTime
            && Objects.equals(path, fileInfo.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(length, path, modifiedTime);
    }
}
