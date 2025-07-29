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

package org.apache.geaflow.runtime.core.context;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.context.AbstractRuntimeContext;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.shuffle.IoDescriptor;

public class DefaultRuntimeContext extends AbstractRuntimeContext {

    private long pipelineId;
    private String pipelineName;
    private TaskArgs taskArgs;
    protected IoDescriptor ioDescriptor;

    public DefaultRuntimeContext(Configuration jobConfig) {
        super(jobConfig);
    }

    @Override
    public long getPipelineId() {
        return pipelineId;
    }

    public DefaultRuntimeContext setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
        return this;
    }

    @Override
    public String getPipelineName() {
        return pipelineName;
    }

    public DefaultRuntimeContext setPipelineName(String jobName) {
        this.pipelineName = jobName;
        return this;
    }

    @Override
    public TaskArgs getTaskArgs() {
        return taskArgs;
    }

    public DefaultRuntimeContext setTaskArgs(TaskArgs taskArgs) {
        this.taskArgs = taskArgs;
        return this;
    }

    @Override
    public Configuration getConfiguration() {
        return this.jobConfig;
    }

    public DefaultRuntimeContext setIoDescriptor(IoDescriptor ioDescriptor) {
        this.ioDescriptor = ioDescriptor;
        return this;
    }

    public DefaultRuntimeContext setWorkPath(String workPath) {
        this.workPath = workPath;
        return this;
    }

    public DefaultRuntimeContext setMetricGroup(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    public DefaultRuntimeContext setWindowId(long windowId) {
        updateWindowId(windowId);
        return this;
    }

    @Override
    public RuntimeContext clone(Map<String, String> opConfig) {
        Map<String, String> newConfig = new HashMap<>();
        newConfig.putAll(jobConfig.getConfigMap());
        newConfig.putAll(opConfig);
        Configuration configuration = new Configuration(newConfig);
        return DefaultRuntimeContext.build(configuration)
            .setTaskArgs(taskArgs)
            .setPipelineId(pipelineId)
            .setPipelineName(pipelineName)
            .setMetricGroup(metricGroup)
            .setIoDescriptor(ioDescriptor)
            .setWorkPath(getWorkPath())
            .setWindowId(windowId);
    }

    public static DefaultRuntimeContext build(Configuration configuration) {
        DefaultRuntimeContext runtimeContext = new DefaultRuntimeContext(configuration);
        return runtimeContext;
    }
}
