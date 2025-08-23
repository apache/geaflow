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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.dal.dao.DataDao;
import org.apache.geaflow.console.common.dal.dao.FunctionDao;
import org.apache.geaflow.console.common.dal.entity.FunctionEntity;
import org.apache.geaflow.console.common.dal.model.FunctionSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.service.converter.DataConverter;
import org.apache.geaflow.console.core.service.converter.FunctionConverter;
import org.apache.geaflow.console.core.service.file.FileRefService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FunctionService extends DataService<GeaflowFunction, FunctionEntity, FunctionSearch> implements FileRefService {

    @Autowired
    private FunctionDao functionDao;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private FunctionConverter functionConverter;

    @Override
    protected DataDao<FunctionEntity, FunctionSearch> getDao() {
        return functionDao;
    }

    @Override
    protected DataConverter<GeaflowFunction, FunctionEntity> getConverter() {
        return functionConverter;
    }

    @Override
    protected List<GeaflowFunction> parse(List<FunctionEntity> functionEntities) {
        // get jar packages
        List<String> packageIds = functionEntities.stream().map(FunctionEntity::getJarPackageId)
            .collect(Collectors.toList());
        List<GeaflowRemoteFile> jarPackages = remoteFileService.get(packageIds);

        Map<String, GeaflowRemoteFile> map = ListUtil.toMap(jarPackages, GeaflowId::getId);

        return functionEntities.stream().map(e -> {
            GeaflowRemoteFile jarPackage = map.get(e.getJarPackageId());
            return functionConverter.convert(e, jarPackage);
        }).collect(Collectors.toList());
    }

    @Override
    public long getFileRefCount(String fileId, String excludeFunctionId) {
        return functionDao.getFileRefCount(fileId, excludeFunctionId);
    }

}

