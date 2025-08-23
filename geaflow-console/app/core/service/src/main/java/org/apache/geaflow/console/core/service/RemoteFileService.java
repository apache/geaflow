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

import java.util.List;
import org.apache.geaflow.console.common.dal.dao.NameDao;
import org.apache.geaflow.console.common.dal.dao.RemoteFileDao;
import org.apache.geaflow.console.common.dal.entity.RemoteFileEntity;
import org.apache.geaflow.console.common.dal.model.RemoteFileSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.service.converter.NameConverter;
import org.apache.geaflow.console.core.service.converter.RemoteFileConverter;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RemoteFileService extends NameService<GeaflowRemoteFile, RemoteFileEntity, RemoteFileSearch> {

    public static final String JAR_FILE_SUFFIX = ".jar";

    @Autowired
    private RemoteFileDao remoteFileDao;

    @Autowired
    private RemoteFileConverter remoteFileConverter;

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Override
    protected List<GeaflowRemoteFile> parse(List<RemoteFileEntity> entities) {
        return ListUtil.convert(entities, e -> remoteFileConverter.convert(e));
    }

    @Override
    public String create(GeaflowRemoteFile model) {
        String name = model.getName();
        if (remoteFileDao.existName(model.getName())) {
            throw new GeaflowIllegalException("File {} exists", name);
        }

        return super.create(model);
    }

    @Override
    protected NameDao<RemoteFileEntity, RemoteFileSearch> getDao() {
        return remoteFileDao;
    }

    @Override
    protected NameConverter<GeaflowRemoteFile, RemoteFileEntity> getConverter() {
        return remoteFileConverter;
    }

    public GeaflowRemoteFile getByName(String name) {
        RemoteFileEntity entity = remoteFileDao.getByName(name);
        return parse(entity);
    }

    public void updateMd5ById(String id, String md5) {
        remoteFileDao.updateMd5(id, md5);
    }

    public void updateUrlById(String id, String url) {
        remoteFileDao.updateUrl(id, url);
    }

    public void validateGetIds(List<String> ids) {
        for (String id : ids) {
            if (!remoteFileDao.validateGetId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)", id);
            }
        }
    }

    public void validateUpdateIds(List<String> ids) {
        for (String id : ids) {
            if (!remoteFileDao.validateUpdateId(id)) {
                throw new GeaflowException("Invalidate id {} (Not system session or current user is not the creator)", id);
            }
        }
    }
}

