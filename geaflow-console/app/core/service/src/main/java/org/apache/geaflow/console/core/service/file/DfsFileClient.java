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

package org.apache.geaflow.console.core.service.file;

import static org.apache.geaflow.console.core.model.plugin.config.DfsPluginConfigClass.DFS_URI_KEY;

import java.io.InputStream;
import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.plugin.GeaflowPlugin;
import org.apache.geaflow.console.core.model.plugin.config.DfsPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class DfsFileClient implements RemoteFileClient {

    private DfsPluginConfigClass dfsConfig;

    private FileSystem fileSystem;

    @Override
    public void init(GeaflowPlugin plugin, GeaflowPluginConfig config) {
        this.dfsConfig = config.getConfig().parse(DfsPluginConfigClass.class);
        GeaflowConfig geaflowConfig = new GeaflowConfig();
        geaflowConfig.put(DFS_URI_KEY, dfsConfig.getDefaultFs());
        geaflowConfig.putAll(dfsConfig.getExtendConfig());

        Configuration conf = new Configuration();
        geaflowConfig.toStringMap().forEach(conf::set);

        try {
            this.fileSystem = FileSystem.get(new URI(dfsConfig.getDefaultFs()), conf);
        } catch (Exception e) {
            throw new GeaflowException("Init DfsFileClient failed", e);
        }
    }

    @Override
    public void upload(String path, InputStream inputStream) {
        String fullPath = getFullPath(path);
        try {
            Path dfsPath = new Path(fullPath);
            FSDataOutputStream outputStream = this.fileSystem.create(dfsPath);
            IOUtils.copyBytes(inputStream, outputStream, 1024 * 1024 * 8, true);
        } catch (Exception e) {
            throw new GeaflowException("Upload file {} failed", fullPath, e);
        }

    }

    @Override
    public InputStream download(String path) {
        String fullPath = getFullPath(path);
        try {
            Path dfsPath = new Path(fullPath);
            if (!fileSystem.exists(dfsPath)) {
                throw new GeaflowException("File doesn't exist {}", fullPath);
            }

            return this.fileSystem.open(dfsPath);
        } catch (Exception e) {
            throw new GeaflowException("Download file {} failed", fullPath, e);
        }
    }

    @Override
    public void delete(String path) {
        String fullPath = getFullPath(path);
        try {
            Path dfsPath = new Path(fullPath);
            if (!fileSystem.exists(dfsPath)) {
                return;
            }

            this.fileSystem.delete(dfsPath, true);
        } catch (Exception e) {
            throw new GeaflowException("Delete file {} failed", fullPath, e);
        }
    }

    @Override
    public String getUrl(String path) {
        return String.format("%s%s", dfsConfig.getDefaultFs(), getFullPath(path));
    }

    @Override
    public boolean checkFileExists(String path) {
        //TODO
        return false;
    }

    public String getFullPath(String path) {
        String root = dfsConfig.getRoot();
        if (!StringUtils.startsWith(root, "/")) {
            throw new GeaflowException("Invalid root config, should start with /");
        }
        root = StringUtils.removeEnd(root, "/");
        return String.format("%s/%s", root, path);
    }
}
