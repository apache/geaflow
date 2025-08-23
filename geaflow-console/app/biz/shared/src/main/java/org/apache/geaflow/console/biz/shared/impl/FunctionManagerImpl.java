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

package org.apache.geaflow.console.biz.shared.impl;

import static org.apache.geaflow.console.core.service.RemoteFileService.JAR_FILE_SUFFIX;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.biz.shared.FunctionManager;
import org.apache.geaflow.console.biz.shared.RemoteFileManager;
import org.apache.geaflow.console.biz.shared.convert.DataViewConverter;
import org.apache.geaflow.console.biz.shared.convert.FunctionViewConverter;
import org.apache.geaflow.console.biz.shared.view.FunctionView;
import org.apache.geaflow.console.biz.shared.view.RemoteFileView;
import org.apache.geaflow.console.common.dal.entity.FunctionEntity;
import org.apache.geaflow.console.common.dal.model.FunctionSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.service.DataService;
import org.apache.geaflow.console.core.service.FunctionService;
import org.apache.geaflow.console.core.service.JobService;
import org.apache.geaflow.console.core.service.RemoteFileService;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class FunctionManagerImpl extends DataManagerImpl<GeaflowFunction, FunctionView, FunctionSearch> implements
    FunctionManager {

    @Autowired
    private FunctionViewConverter functionViewConverter;

    @Autowired
    private FunctionService functionService;

    @Autowired
    private JobService jobService;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private RemoteFileManager remoteFileManager;

    @Override
    protected DataViewConverter<GeaflowFunction, FunctionView> getConverter() {
        return functionViewConverter;
    }

    @Override
    protected DataService<GeaflowFunction, FunctionEntity, FunctionSearch> getService() {
        return functionService;
    }

    @Override
    protected List<GeaflowFunction> parse(List<FunctionView> views) {
        List<String> packageIds = views.stream().map(e -> e.getJarPackage().getId()).collect(Collectors.toList());
        List<GeaflowRemoteFile> jarPackages = remoteFileService.get(packageIds);
        Map<String, GeaflowRemoteFile> map = jarPackages.stream()
            .collect(Collectors.toMap(GeaflowId::getId, e -> e));

        return views.stream().map(e -> {
            GeaflowRemoteFile jarPackage = map.get(e.getJarPackage().getId());
            return functionViewConverter.convert(e, jarPackage);
        }).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public String createFunction(String instanceName, FunctionView functionView, MultipartFile functionFile, String fileId) {
        String functionName = functionView.getName();
        if (StringUtils.isBlank(functionName)) {
            throw new GeaflowIllegalException("Invalid function name");
        }

        if (functionService.existName(functionName)) {
            throw new GeaflowIllegalException("Function name {} exists", functionName);
        }

        Preconditions.checkNotNull(functionView.getEntryClass(), "Function needs entryClass");
        if (fileId == null) {
            Preconditions.checkNotNull(functionFile, "Invalid function file");
            functionView.setJarPackage(createRemoteFile(functionFile));
        } else {
            // bind a jar file if jarId is not null
            if (!remoteFileService.exist(fileId)) {
                throw new GeaflowIllegalException("File {} does not exist", fileId);
            }
            RemoteFileView remoteFileView = new RemoteFileView();
            remoteFileView.setId(fileId);
            functionView.setJarPackage(remoteFileView);
        }

        return super.create(instanceName, functionView);
    }

    @Override
    @Transactional
    public boolean updateFunction(String instanceName, String functionName, FunctionView updateView, MultipartFile functionFile) {
        FunctionView oldView = getByName(instanceName, functionName);
        if (oldView == null) {
            throw new GeaflowIllegalException("Function name {} not exists", functionName);
        }

        if (functionFile != null) {
            updateView.setJarPackage(updateJarFile(updateView, functionFile));
        }
        return updateById(oldView.getId(), updateView);
    }

    @Transactional
    @Override
    public boolean deleteFunction(String instanceName, String functionName) {
        String instanceId = getInstanceIdByName(instanceName);
        GeaflowFunction function = functionService.getByName(instanceId, functionName);
        if (function == null) {
            return false;
        }

        // check plugin is used by jobs
        List<String> jobIds = jobService.getJobByResources(function.getName(), function.getInstanceId(),
            GeaflowResourceType.FUNCTION);
        if (CollectionUtils.isNotEmpty(jobIds)) {
            List<String> jobNames = ListUtil.convert(jobIds, e -> jobService.getNameById(e));
            throw new GeaflowException("Function {} is used by job: {}", function.getName(), String.join(",", jobNames));
        }

        GeaflowRemoteFile file = function.getJarPackage();
        if (file != null) {
            // do not delete if file is used by others
            try {
                remoteFileManager.deleteRefJar(file.getId(), function.getId(), GeaflowResourceType.FUNCTION);

            } catch (Exception e) {
                log.info(" Delete function -> delete file {} failed ", file.getName(), e);
            }
        }
        return super.dropByName(instanceName, functionName);
    }


    private RemoteFileView createRemoteFile(MultipartFile functionFile) {
        if (!StringUtils.endsWith(functionFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        String fileName = functionFile.getOriginalFilename();
        if (remoteFileService.existName(fileName)) {
            throw new GeaflowException("FileName {} exists", fileName);
        }

        String path = RemoteFileStorage.getUserFilePath(ContextHolder.get().getUserId(), fileName);

        RemoteFileView remoteFileView = new RemoteFileView();
        remoteFileView.setName(fileName);
        remoteFileView.setPath(path);
        remoteFileManager.create(remoteFileView, functionFile);

        return remoteFileView;
    }

    private RemoteFileView updateJarFile(FunctionView functionView, MultipartFile multipartFile) {
        if (!StringUtils.endsWith(multipartFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        RemoteFileView remoteFileView = functionView.getJarPackage();
        if (remoteFileView == null) {
            return createRemoteFile(multipartFile);

        } else {
            String remoteFileId = remoteFileView.getId();
            remoteFileManager.upload(remoteFileId, multipartFile);
            return remoteFileView;
        }
    }
}
