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

package org.apache.geaflow.cluster.driver;

import static org.apache.geaflow.cluster.failover.FailoverStrategyType.component_fo;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.cluster.common.ExecutionIdGenerator;
import org.apache.geaflow.cluster.common.IReliableContext;
import org.apache.geaflow.cluster.common.ReliableContainerContext;
import org.apache.geaflow.cluster.constants.ClusterConstants;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverContext extends ReliableContainerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);

    private Pipeline pipeline;

    public List<Long> getPipelineTaskIds() {
        return pipelineTaskIds;
    }

    private List<Long> pipelineTaskIds;
    private List<Integer> finishedPipelineTasks;
    private int index;

    public DriverContext(int id, int index, Configuration config) {
        super(id, ClusterConstants.getDriverName(id), config);
        this.index = index;
        this.finishedPipelineTasks = new ArrayList<>();
        this.pipelineTaskIds = new ArrayList<>();
    }

    public DriverContext(int id, int index, Configuration config, boolean isRecover) {
        this(id, index, config);
        this.isRecover = isRecover;
    }

    @Override
    public void load() {
        Pipeline pipeline =
            ClusterMetaStore.getInstance(id, name, config).getPipeline();
        if (pipeline != null) {
            List<Integer> finishedPipelineTasks = ClusterMetaStore.getInstance().getPipelineTasks();
            if (finishedPipelineTasks == null) {
                finishedPipelineTasks = new ArrayList<>();
            }
            List<Long> pipelineTaskIds = ClusterMetaStore.getInstance().getPipelineTaskIds();
            if (pipeline.getPipelineTaskList() != null && pipelineTaskIds == null) {
                throw new GeaflowRuntimeException(String.format("driver %s recover context %s "
                    + "error: pipeline task ids is null", id, this));
            }
            this.pipeline = pipeline;
            this.finishedPipelineTasks = finishedPipelineTasks;
            this.pipelineTaskIds = pipelineTaskIds;
            LOGGER.info("driver {} recover context {} pipeline {} finishedPipelineTasks {} pipelineTaskIds {}",
                id, this, pipeline, finishedPipelineTasks, pipelineTaskIds);
        }
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void addPipeline(Pipeline pipeline) {
        genPipelineTaskIds(pipeline);
        validatePipeline(pipeline);
        if (!pipeline.equals(this.pipeline)) {
            this.pipeline = pipeline;
        }
    }

    public int getIndex() {
        return index;
    }

    public List<Integer> getFinishedPipelineTasks() {
        return finishedPipelineTasks;
    }

    public void addFinishedPipelineTask(int pipelineTaskIndex) {
        if (!finishedPipelineTasks.contains(pipelineTaskIndex)) {
            finishedPipelineTasks.add(pipelineTaskIndex);
        }
    }

    private void validatePipeline(Pipeline pipeline) {
        // Given that partial components fo only supported for pipeline service,
        // do validation for pipeline.
        if (!pipeline.getPipelineTaskList().isEmpty()
            && config.getString(FO_STRATEGY).equalsIgnoreCase(component_fo.name())) {
            throw new GeaflowRuntimeException("not support component_fo for executing pipeline tasks");
        }
    }

    public static class PipelineCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            DriverContext driverContext = ((DriverContext) context);
            if (driverContext.getPipeline() != null) {
                ClusterMetaStore.getInstance().savePipeline(driverContext.getPipeline()).flush();
                ClusterMetaStore.getInstance().savePipelineTaskIds(driverContext.getPipelineTaskIds()).flush();
                LOGGER.info("driver {} checkpoint context {} pipeline {}, PipelineTaskIds {}",
                    driverContext.getId(), driverContext, driverContext.getPipeline(), driverContext.getPipelineTaskIds());
            }
        }
    }

    public static class PipelineTaskCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            DriverContext driverContext = ((DriverContext) context);
            if (driverContext.getFinishedPipelineTasks() != null && !driverContext.getFinishedPipelineTasks().isEmpty()) {
                ClusterMetaStore.getInstance().savePipelineTasks(driverContext.getFinishedPipelineTasks()).flush();
                LOGGER.info("driver {} checkpoint pipeline finished tasks {}",
                    driverContext.getId(), driverContext.getFinishedPipelineTasks());
            }
        }
    }

    private void genPipelineTaskIds(Pipeline pipeline) {
        // When recover, we need not generate pipeline task ids.
        if (this.pipelineTaskIds.isEmpty()) {
            for (int i = 0, size = pipeline.getPipelineTaskList().size(); i < size; i++) {
                this.pipelineTaskIds.add(ExecutionIdGenerator.getInstance().generateId());
            }
        }
    }
}
