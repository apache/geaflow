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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.fetcher.FetchRequest;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.runtime.core.worker.AbstractAlignedWorker;
import org.apache.geaflow.runtime.core.worker.AbstractWorker;
import org.apache.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import org.apache.geaflow.runtime.core.worker.context.WorkerContext;
import org.apache.geaflow.shuffle.message.PipelineMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIterationComputeCommand extends AbstractExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIterationComputeCommand.class);

    protected long fetchWindowId;
    protected long fetchCount;
    protected Map<Long, Long> windowCount;
    protected Map<Long, List<PipelineMessage>> batchMessageCache;

    public AbstractIterationComputeCommand(long schedulerId, int workerId, int cycleId, long windowId, long fetchWindowId, long fetchCount) {
        super(schedulerId, workerId, cycleId, windowId);
        this.fetchWindowId = fetchWindowId;
        this.fetchCount = fetchCount;
        this.windowCount = new HashMap<>();
        this.batchMessageCache = new HashMap();
    }

    @Override
    public void execute(ITaskContext taskContext) {
        final long start = System.currentTimeMillis();
        super.execute(taskContext);
        AbstractWorker abstractWorker = (AbstractWorker) worker;
        abstractWorker.init(windowId);
        fetcherRunner.add(new FetchRequest(((WorkerContext) this.context).getTaskId(), fetchWindowId, fetchCount));
        abstractWorker.process(fetchCount,
            this instanceof LoadGraphProcessEvent || worker instanceof AbstractAlignedWorker);
        ((AbstractWorkerContext) this.context).getEventMetrics().addProcessCostMs(System.currentTimeMillis() - start);
    }

}
