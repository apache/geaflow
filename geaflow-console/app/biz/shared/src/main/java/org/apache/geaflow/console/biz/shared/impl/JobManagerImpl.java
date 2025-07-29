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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.biz.shared.JobManager;
import org.apache.geaflow.console.biz.shared.RemoteFileManager;
import org.apache.geaflow.console.biz.shared.TaskManager;
import org.apache.geaflow.console.biz.shared.convert.IdViewConverter;
import org.apache.geaflow.console.biz.shared.convert.JobViewConverter;
import org.apache.geaflow.console.biz.shared.view.GraphView;
import org.apache.geaflow.console.biz.shared.view.IdView;
import org.apache.geaflow.console.biz.shared.view.JobView;
import org.apache.geaflow.console.biz.shared.view.RemoteFileView;
import org.apache.geaflow.console.common.dal.entity.JobEntity;
import org.apache.geaflow.console.common.dal.model.JobSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.common.util.type.GeaflowTaskType;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowStruct;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.job.GeaflowTransferJob.StructMapping;
import org.apache.geaflow.console.core.service.AuthorizationService;
import org.apache.geaflow.console.core.service.IdService;
import org.apache.geaflow.console.core.service.JobService;
import org.apache.geaflow.console.core.service.ReleaseService;
import org.apache.geaflow.console.core.service.RemoteFileService;
import org.apache.geaflow.console.core.service.StatementService;
import org.apache.geaflow.console.core.service.TableService;
import org.apache.geaflow.console.core.service.TaskService;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class JobManagerImpl extends IdManagerImpl<GeaflowJob, JobView, JobSearch> implements JobManager {

    @Autowired
    private JobService jobService;

    @Autowired
    private JobViewConverter jobViewConverter;

    @Autowired
    private ReleaseService releaseService;

    @Autowired
    private TaskManager taskManager;

    @Autowired
    private TaskService taskService;

    @Autowired
    private RemoteFileManager remoteFileManager;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private StatementService statementService;

    @Autowired
    private TableService tableService;

    @Override
    public IdViewConverter<GeaflowJob, JobView> getConverter() {
        return jobViewConverter;
    }

    @Override
    public IdService<GeaflowJob, JobEntity, JobSearch> getService() {
        return jobService;
    }

    @Override
    public List<GeaflowJob> parse(List<JobView> views) {
        return ListUtil.convert(views, v -> {
            GeaflowJobType type = v.getType();
            Preconditions.checkNotNull(type, "job Type is null");
            switch (type) {
                case PROCESS:
                    // get functions
                    List<String> functionIds = ListUtil.convert(v.getFunctions(), IdView::getId);
                    List<GeaflowFunction> functions = jobService.getResourceService(GeaflowResourceType.FUNCTION).get(functionIds);
                    return jobViewConverter.convert(v, null, null, functions, null);
                case CUSTOM:
                    GeaflowRemoteFile remoteFile = Optional.ofNullable(v.getJarPackage())
                        .map(e -> remoteFileService.get(e.getId())).orElse(null);
                    return jobViewConverter.convert(v, null, null, null, remoteFile);
                case INTEGRATE:
                case SERVE:
                    List<GeaflowStruct> structs = null;
                    if (type == GeaflowJobType.INTEGRATE) {
                        // get tables
                        structs = getStructs(v);
                    }
                    Preconditions.checkArgument(v.getGraphs() != null && v.getGraphs().size() == 1,
                        "Must have one graph");

                    List<String> graphIds = ListUtil.convert(v.getGraphs(), IdView::getId);
                    List<GeaflowGraph> graphs = ListUtil.convert(graphIds, id -> {
                        GeaflowGraph g = (GeaflowGraph) jobService.getResourceService(GeaflowResourceType.GRAPH).get(id);
                        Preconditions.checkNotNull(g, "Graph id {} is null", id);
                        return g;
                    });
                    return jobViewConverter.convert(v, structs, graphs, null, null);

                default:
                    throw new GeaflowException("Unsupported job Type: {}", v.getType());
            }
        });
    }

    private List<GeaflowStruct> getStructs(JobView jobView) {
        List<StructMapping> structMappings = JSON.parseObject(jobView.getStructMappings(),
            new TypeReference<List<StructMapping>>() {
            });
        Set<String> tableNames = structMappings.stream().map(StructMapping::getTableName).collect(Collectors.toSet());
        return tableNames.stream().map(e -> tableService.getByName(jobView.getInstanceId(), e))
            .collect(Collectors.toList());
    }


    @Override
    @Transactional
    public boolean drop(List<String> jobIds) {
        List<String> taskIds = taskService.getIdsByJob(jobIds);
        taskManager.drop(taskIds);
        releaseService.dropByJobIds(jobIds);
        jobService.dropResources(jobIds);
        authorizationService.dropByResources(jobIds, GeaflowResourceType.JOB);
        statementService.dropByJobIds(jobIds);
        try {
            Map<String, String> jarIds = jobService.getJarIds(jobIds);
            for (String jobId : jobIds) {
                remoteFileManager.deleteRefJar(jarIds.get(jobId), jobId, GeaflowResourceType.JOB);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }

        return super.drop(jobIds);
    }


    private String createApiJob(JobView jobView, MultipartFile jarFile, String fileId) {
        String jobName = jobView.getName();
        if (StringUtils.isBlank(jobName)) {
            throw new GeaflowIllegalException("Invalid function name");
        }

        if (jobService.existName(jobName)) {
            throw new GeaflowIllegalException("Job name {} exists", jobName);
        }

        if (jobView.getType() == GeaflowJobType.CUSTOM) {
            Preconditions.checkNotNull(jobView.getEntryClass(), "Custom job needs entryClass");
        }

        if (jarFile != null) {
            jobView.setJarPackage(createRemoteFile(jarFile));
        } else if (fileId != null) {
            // bind a jar file if jarId is not null
            if (!remoteFileService.exist(fileId)) {
                throw new GeaflowIllegalException("File {} does not exist", fileId);
            }
            RemoteFileView remoteFileView = new RemoteFileView();
            remoteFileView.setId(fileId);
            jobView.setJarPackage(remoteFileView);
        }

        // job package could be null
        return super.create(jobView);
    }


    private boolean updateApiJob(String jobId, JobView updateView, MultipartFile jarFile, String fileId) {
        if (updateView.getType() == GeaflowJobType.CUSTOM) {
            Preconditions.checkNotNull(updateView.getEntryClass(), "Hla job needs entryClass");
        }

        if (jarFile != null) {
            updateView.setJarPackage(createRemoteFile(jarFile));

            // try to delete old file
            GeaflowJob job = jobService.get(jobId);
            GeaflowRemoteFile oldJar = job.getJarPackage();
            if (oldJar != null) {
                String oldJarId = job.getJarPackage().getId();
                try {
                    remoteFileManager.deleteRefJar(oldJarId, jobId, GeaflowResourceType.JOB);
                } catch (Exception e) {
                    log.info("delete job jar fail, jobName: {}, jarId: {}", job.getName(), oldJarId);
                }
            }

        } else if (fileId != null) {
            // bind a jar file if jarId is not null
            if (!remoteFileService.exist(fileId)) {
                throw new GeaflowIllegalException("File {} does not exist", fileId);
            }
            RemoteFileView remoteFileView = new RemoteFileView();
            remoteFileView.setId(fileId);
            updateView.setJarPackage(remoteFileView);
        }

        return updateById(jobId, updateView);
    }

    private RemoteFileView createRemoteFile(MultipartFile jarFile) {
        if (!StringUtils.endsWith(jarFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        String fileName = jarFile.getOriginalFilename();
        if (remoteFileService.existName(fileName)) {
            throw new GeaflowException("FileName {} exists", fileName);
        }

        String path = RemoteFileStorage.getUserFilePath(ContextHolder.get().getUserId(), fileName);

        RemoteFileView remoteFileView = new RemoteFileView();
        remoteFileView.setName(fileName);
        remoteFileView.setPath(path);
        remoteFileManager.create(remoteFileView, jarFile);

        return remoteFileView;
    }

    @Override
    @Transactional
    public String create(JobView jobView, MultipartFile jarFile, String fileId, List<String> graphIds) {
        Preconditions.checkNotNull(jobView.getType(), "Job type is null");
        if (CollectionUtils.isNotEmpty(graphIds)) {
            List<GraphView> graphViews = ListUtil.convert(graphIds, id -> {
                GraphView graphView = new GraphView();
                graphView.setId(id);
                return graphView;
            });
            jobView.setGraphs(graphViews);
        }
        return jobView.getType().getTaskType() == GeaflowTaskType.API ? createApiJob(jobView, jarFile, fileId) :
            super.create(jobView);
    }

    @Override
    public boolean update(String jobId, JobView jobView, MultipartFile jarFile, String fileId) {
        Preconditions.checkNotNull(jobView.getType(), "Job type is null");
        return jobView.getType().getTaskType() == GeaflowTaskType.API ? updateApiJob(jobId, jobView, jarFile, fileId) :
            super.updateById(jobId, jobView);
    }
}
