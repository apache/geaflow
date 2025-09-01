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

import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.cluster.response.ShardResult;
import org.apache.geaflow.collector.IResultCollector;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.ForwardOutputDesc;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.apache.geaflow.shuffle.message.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPipelineOutputCollector<T>
    extends AbstractPipelineCollector<T> implements IResultCollector<ShardResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPipelineOutputCollector.class);

    private int edgeId;
    private String edgeName;
    private Shard shard;
    private OutputType collectType;

    public AbstractPipelineOutputCollector(ForwardOutputDesc outputDesc) {
        super(outputDesc);
        this.edgeName = outputDesc.getEdgeName();
        this.edgeId = outputDesc.getEdgeId();
        this.collectType = outputDesc.getType();
    }

    @Override
    public void setUp(RuntimeContext runtimeContext) {
        super.setUp(runtimeContext);
        int taskId = runtimeContext.getTaskArgs().getTaskId();
        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        this.shard = null;
        LOGGER.info("setup PipelineOutputCollector {} taskId {} taskIndex {} edgeName {} edgeId {}",
            this, taskId, taskIndex, this.edgeName, this.edgeId);
    }

    @Override
    public void finish() {
        try {
            this.shard = (Shard) this.outputBuffer.finish(this.windowId);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public String getTag() {
        return edgeName;
    }

    /**
     * Collect shard result.
     *
     * @return
     */
    @Override
    public ShardResult collectResult() {
        if (shard == null) {
            return null;
        }
        return new ShardResult(shard.getEdgeId(), collectType, shard.getSlices());
    }
}
