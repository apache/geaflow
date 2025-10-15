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

package org.apache.geaflow.runtime.core.protocol;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.cluster.collector.CollectResponseCollector;
import org.apache.geaflow.cluster.collector.InitEmitterRequest;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.apache.geaflow.shuffle.IoDescriptor;
import org.apache.geaflow.shuffle.OutputDescriptor;
import org.apache.geaflow.shuffle.ResponseOutputDesc;

/**
 * An assign event provides some runtime execution information for worker to build the cycle pipeline.
 * including: execution task descriptors, shuffle descriptors
 */
public class InitCollectCycleEvent extends InitCycleEvent {

    private static final int COLLECT_BUCKET_NUM = 1;

    public InitCollectCycleEvent(long schedulerId,
                                 int workerId,
                                 int cycleId,
                                 long iterationId,
                                 long pipelineId,
                                 String pipelineName,
                                 IoDescriptor ioDescriptor,
                                 ExecutionTask task,
                                 String driverId,
                                 HighAvailableLevel haLevel) {
        super(schedulerId, workerId, cycleId, iterationId, pipelineId, pipelineName, ioDescriptor, task, driverId, haLevel);
    }

    @Override
    protected List<ICollector<?>> buildCollectors(OutputDescriptor outputDescriptor, InitEmitterRequest request) {
        Preconditions.checkArgument(outputDescriptor.getOutputDescList().size() == COLLECT_BUCKET_NUM,
            "only support one collect output info yet");
        ResponseOutputDesc outputDesc = (ResponseOutputDesc) outputDescriptor.getOutputDescList().get(0);
        return Collections.singletonList(new CollectResponseCollector<>(outputDesc));
    }

}
