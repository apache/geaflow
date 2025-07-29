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

package org.apache.geaflow.console.core.service;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.dal.dao.NameDao;
import org.apache.geaflow.console.common.dal.dao.VersionDao;
import org.apache.geaflow.console.common.dal.entity.VersionEntity;
import org.apache.geaflow.console.common.dal.model.VersionSearch;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.apache.geaflow.console.core.service.converter.NameConverter;
import org.apache.geaflow.console.core.service.converter.VersionConverter;
import org.apache.geaflow.console.core.service.file.FileRefService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class VersionService extends NameService<GeaflowVersion, VersionEntity, VersionSearch> implements FileRefService {

    @Autowired
    private VersionDao versionDao;

    @Autowired
    private VersionConverter versionConverter;

    @Autowired
    private RemoteFileService remoteFileService;

    @Override
    protected NameDao<VersionEntity, VersionSearch> getDao() {
        return versionDao;
    }

    @Override
    protected NameConverter<GeaflowVersion, VersionEntity> getConverter() {
        return versionConverter;
    }

    @Override
    protected List<GeaflowVersion> parse(List<VersionEntity> versionEntities) {
        return versionEntities.stream().map(e -> {
            GeaflowRemoteFile engineJar = remoteFileService.get(e.getEngineJarId());
            GeaflowRemoteFile langJar = remoteFileService.get(e.getLangJarId());
            return versionConverter.convert(e, engineJar, langJar);
        }).collect(Collectors.toList());
    }

    public GeaflowVersion getDefaultVersion() {
        VersionEntity version = versionDao.getDefaultVersion();
        Preconditions.checkNotNull(version, "No default published version found");
        return parse(version);
    }

    public GeaflowVersion getPublishVersionByName(String name) {
        return parse(versionDao.getPublishVersionByName(name));
    }

    @Override
    public long getFileRefCount(String fileId, String versionId) {
        return versionDao.getFileRefCount(fileId, versionId);
    }
}

