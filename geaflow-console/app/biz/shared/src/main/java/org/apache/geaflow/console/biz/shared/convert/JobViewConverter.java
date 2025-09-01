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

package org.apache.geaflow.console.biz.shared.convert;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.geaflow.console.biz.shared.view.FunctionView;
import org.apache.geaflow.console.biz.shared.view.GraphView;
import org.apache.geaflow.console.biz.shared.view.JobView;
import org.apache.geaflow.console.biz.shared.view.StructView;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;
import org.apache.geaflow.console.core.model.code.GeaflowCode;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowStruct;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.job.GeaflowCustomJob;
import org.apache.geaflow.console.core.model.job.GeaflowIntegrateJob;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.job.GeaflowProcessJob;
import org.apache.geaflow.console.core.model.job.GeaflowServeJob;
import org.apache.geaflow.console.core.model.job.GeaflowTransferJob.FieldMappingItem;
import org.apache.geaflow.console.core.model.job.GeaflowTransferJob.StructMapping;
import org.apache.geaflow.console.core.service.InstanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobViewConverter extends NameViewConverter<GeaflowJob, JobView> {

    @Autowired
    private GraphViewConverter graphViewConverter;

    @Autowired
    private VertexViewConverter vertexViewConverter;

    @Autowired
    private EdgeViewConverter edgeViewConverter;

    @Autowired
    private TableViewConverter tableViewConverter;

    @Autowired
    private RemoteFileViewConverter remoteFileViewConverter;

    @Autowired
    private FunctionViewConverter functionViewConverter;

    @Autowired
    private InstanceService instanceService;

    private final Map<GeaflowStructType, StructViewConverter> converterMap = new HashMap<>();

    @PostConstruct
    public void init() {
        converterMap.put(GeaflowStructType.VERTEX, vertexViewConverter);
        converterMap.put(GeaflowStructType.EDGE, edgeViewConverter);
        converterMap.put(GeaflowStructType.TABLE, tableViewConverter);
    }

    @Override
    public void merge(JobView view, JobView updateView) {
        super.merge(view, updateView);
        switch (view.getType()) {
            case INTEGRATE:
                Optional.ofNullable(updateView.getStructMappings()).ifPresent(view::setStructMappings);
                Optional.ofNullable(updateView.getGraphs()).ifPresent(view::setGraphs);
                break;
            case PROCESS:
                Optional.ofNullable(updateView.getUserCode()).ifPresent(view::setUserCode);
                break;
            case CUSTOM:
                Optional.ofNullable(updateView.getEntryClass()).ifPresent(view::setEntryClass);
                Optional.ofNullable(updateView.getJarPackage()).ifPresent(view::setJarPackage);
                break;
            case SERVE:
                break;
            default:
                throw new GeaflowException("Unsupported job type: {}", view.getType());
        }
    }

    @Override
    protected JobView modelToView(GeaflowJob model) {
        JobView jobView = super.modelToView(model);
        jobView.setUserCode(Optional.ofNullable(model.getUserCode()).map(GeaflowCode::getText).orElse(null));

        List<GraphView> graphs = ListUtil.convert(model.getGraphs(), e -> graphViewConverter.convert(e));

        List<StructView> structs = ListUtil.convert(model.getStructs(), e -> (StructView) converterMap.get(e.getType()).convert(e));

        jobView.setStructs(structs);
        jobView.setGraphs(graphs);

        jobView.setStructMappings(JSON.toJSONString(model.getStructMappings()));

        List<FunctionView> functions = ListUtil.convert(model.getFunctions(), e -> functionViewConverter.convert(e));
        jobView.setFunctions(functions);
        jobView.setType(model.getType());
        jobView.setInstanceId(model.getInstanceId());
        jobView.setInstanceName(instanceService.get(model.getInstanceId()).getName());
        jobView.setEntryClass(model.getEntryClass());
        jobView.setJarPackage(Optional.ofNullable(model.getJarPackage()).map(e -> remoteFileViewConverter.convert(e)).orElse(null));
        return jobView;
    }

    public GeaflowJob convert(JobView view, List<GeaflowStruct> structs, List<GeaflowGraph> graphs,
                              List<GeaflowFunction> functions, GeaflowRemoteFile jarFile) {
        GeaflowJobType jobType = view.getType();
        GeaflowJob job;
        switch (jobType) {
            case INTEGRATE:
                GeaflowIntegrateJob integrateJob = (GeaflowIntegrateJob) viewToModel(view, GeaflowIntegrateJob.class);

                Preconditions.checkNotNull(view.getStructMappings());

                List<StructMapping> structMappings = JSON.parseArray(view.getStructMappings(), StructMapping.class);
                // dedup duplicated field mappings
                for (StructMapping structMapping : structMappings) {
                    List<FieldMappingItem> distinctMapping = structMapping.getFieldMappings().stream().distinct().collect(Collectors.toList());
                    structMapping.setFieldMappings(distinctMapping);
                }
                integrateJob.setStructMappings(structMappings);
                integrateJob.setGraph(graphs);
                integrateJob.setStructs(structs);
                job = integrateJob;
                break;
            case PROCESS:
                GeaflowProcessJob processJob = (GeaflowProcessJob) viewToModel(view, GeaflowProcessJob.class);
                processJob.setUserCode(view.getUserCode());
                processJob.setFunctions(functions);
                job = processJob;
                break;
            case CUSTOM:
                GeaflowCustomJob customJob = (GeaflowCustomJob) viewToModel(view, GeaflowCustomJob.class);
                customJob.setEntryClass(view.getEntryClass());
                customJob.setJarPackage(jarFile);
                job = customJob;
                break;
            case SERVE:
                GeaflowServeJob serveJob = (GeaflowServeJob) viewToModel(view, GeaflowServeJob.class);
                serveJob.setGraph(graphs);
                job = serveJob;
                break;
            default:
                throw new GeaflowException("Unsupported job type: {}", jobType);
        }

        job.setInstanceId(view.getInstanceId());
        job.setType(jobType);
        //TODO job.setSla(entity.getSlaId());
        return job;
    }

}
