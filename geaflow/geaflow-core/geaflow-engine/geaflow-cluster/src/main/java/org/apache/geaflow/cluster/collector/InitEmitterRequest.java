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

package org.apache.geaflow.cluster.collector;

import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.shuffle.OutputDescriptor;
import org.apache.geaflow.shuffle.message.Shard;

public class InitEmitterRequest extends AbstractEmitterRequest {

    private final Configuration configuration;
    private final long pipelineId;
    private final String pipelineName;
    private final TaskArgs taskArgs;
    private final OutputDescriptor outputDescriptor;
    private final List<IOutputMessageBuffer<?, Shard>> outputBuffers;

    public InitEmitterRequest(Configuration configuration,
                              long windowId,
                              long pipelineId,
                              String pipelineName,
                              TaskArgs taskArgs,
                              OutputDescriptor outputDescriptor,
                              List<IOutputMessageBuffer<?, Shard>> outputBuffers) {
        super(taskArgs.getTaskId(), windowId);
        this.configuration = configuration;
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.taskArgs = taskArgs;
        this.outputDescriptor = outputDescriptor;
        this.outputBuffers = outputBuffers;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public long getPipelineId() {
        return this.pipelineId;
    }

    public String getPipelineName() {
        return this.pipelineName;
    }

    public TaskArgs getTaskArgs() {
        return this.taskArgs;
    }

    public OutputDescriptor getOutputDescriptor() {
        return this.outputDescriptor;
    }

    public List<IOutputMessageBuffer<?, Shard>> getOutputBuffers() {
        return this.outputBuffers;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.INIT;
    }

}
