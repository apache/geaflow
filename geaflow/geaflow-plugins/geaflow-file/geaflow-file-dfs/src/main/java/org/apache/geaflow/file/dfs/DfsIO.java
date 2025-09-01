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

package org.apache.geaflow.file.dfs;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DfsIO implements IPersistentIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(DfsIO.class);
    private static final String DFS_URI_KEY = "fs.defaultFS";
    protected static final String LOCAL_FILE_IMPL = "fs.file.impl";
    protected FileSystem fileSystem;

    public DfsIO() {

    }

    public void init(org.apache.geaflow.common.config.Configuration userConfig) {
        String jsonConfig = Preconditions.checkNotNull(userConfig.getString(FileConfigKeys.JSON_CONFIG));
        Map<String, String> persistConfig = GsonUtil.parse(jsonConfig);
        Preconditions.checkArgument(persistConfig.containsKey(DFS_URI_KEY), DFS_URI_KEY + " must be set");
        Configuration conf = new Configuration();
        conf.set(LOCAL_FILE_IMPL, LocalFileSystem.class.getCanonicalName());
        for (Entry<String, String> entry : persistConfig.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        try {
            this.fileSystem = FileSystem.newInstance(getURIFromConf(conf), conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected URI getURIFromConf(Configuration configuration) throws URISyntaxException {
        return new URI(configuration.get(DFS_URI_KEY));
    }

    @Override
    public List<String> listFileName(Path path) throws IOException {
        List<String> fileNames = new ArrayList<>();
        for (FileStatus status : fileSystem.listStatus(path)) {
            fileNames.add(status.getPath().getName());
        }
        return fileNames;
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return fileSystem.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        if (exists(path)) {
            return this.fileSystem.delete(path, recursive);
        }

        return false;
    }

    @Override
    public boolean renameFile(Path from, Path to) throws IOException {
        if (exists(to)) {
            this.fileSystem.delete(to, false);
        }
        return fileSystem.rename(from, to);
    }

    @Override
    public boolean createNewFile(Path path) throws IOException {
        if (!fileSystem.exists(path.getParent())) {
            fileSystem.mkdirs(path.getParent());
        }
        return fileSystem.createNewFile(path);
    }

    @Override
    public void copyFromLocalFile(Path local, Path remote) throws IOException {
        fileSystem.copyFromLocalFile(false, true, local, remote);
    }

    @Override
    public void copyToLocalFile(Path remote, Path local) throws IOException {
        fileSystem.copyToLocalFile(false, remote, local, true);
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        FileStatus status = fileSystem.getFileStatus(path);
        return status.getLen();
    }

    @Override
    public long getFileCount(Path path) throws IOException {
        ContentSummary summary = this.fileSystem.getContentSummary(path);
        return summary.getFileCount();
    }

    @Override
    public FileInfo getFileInfo(Path path) throws IOException {
        FileStatus status = fileSystem.getFileStatus(path);
        return FileInfo.of(status);
    }

    @Override
    public FileInfo[] listFileInfo(Path path, PathFilter filter) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(path, filter);
        FileInfo[] fileInfos = new FileInfo[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            fileInfos[i] = FileInfo.of(fileStatuses[i]);
        }
        return fileInfos;
    }

    @Override
    public FileInfo[] listFileInfo(Path path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        FileInfo[] fileInfos = new FileInfo[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            fileInfos[i] = FileInfo.of(fileStatuses[i]);
        }
        return fileInfos;
    }

    @Override
    public InputStream open(Path path) throws IOException {
        return fileSystem.open(path);
    }

    @Override
    public void close() throws IOException {
        this.fileSystem.close();
    }

    @Override
    public PersistentType getPersistentType() {
        return PersistentType.DFS;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }
}
