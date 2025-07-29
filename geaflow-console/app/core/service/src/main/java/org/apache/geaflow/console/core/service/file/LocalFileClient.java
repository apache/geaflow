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

import java.io.InputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.FileUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.plugin.GeaflowPlugin;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.model.plugin.config.LocalPluginConfigClass;
import org.apache.geaflow.console.core.model.task.GeaflowTask;

public class LocalFileClient implements RemoteFileClient {

    private final String gatewayUrl;

    private LocalPluginConfigClass localConfig;

    public LocalFileClient(String gatewayUrl) {
        this.gatewayUrl = gatewayUrl;
    }

    @Override
    public void init(GeaflowPlugin plugin, GeaflowPluginConfig config) {
        this.localConfig = config.getConfig().parse(LocalPluginConfigClass.class);
        FileUtil.mkdir(localConfig.getRoot());
    }

    @Override
    public void upload(String path, InputStream inputStream) {
        FileUtil.writeFile(getFullPath(path), inputStream);
    }

    @Override
    public InputStream download(String path) {
        return FileUtil.readFileStream(getFullPath(path));
    }

    @Override
    public void delete(String path) {
        FileUtil.delete(getFullPath(path));
    }

    @Override
    public String getUrl(String path) {
        return GeaflowTask.getTaskFileUrlFormatter(gatewayUrl, getFullPath(path));
    }

    @Override
    public boolean checkFileExists(String path) {
        //TODO
        return false;
    }

    public String getFullPath(String path) {
        String root = localConfig.getRoot();
        if (!StringUtils.startsWith(root, "/")) {
            throw new GeaflowException("Invalid root config, should start with /");
        }
        root = StringUtils.removeEnd(root, "/");

        return String.format("%s/%s", root, path);
    }

}
