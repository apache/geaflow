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

package org.apache.geaflow.cluster.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.cluster.rpc.ConnectAddress;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.ExecutorUtil;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncPipelineClient extends AbstractPipelineClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncPipelineClient.class);

    private static final String PREFIX_DRIVER_EXECUTE_PIPELINE = "driver-submit-pipeline-";

    private ExecutorService executorService;

    @Override
    public IPipelineResult submit(Pipeline pipeline) {
        int driverNum = driverAddresses.size();
        executorService = new ThreadPoolExecutor(driverNum, driverNum, 0,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(driverNum),
            ThreadUtil.namedThreadFactory(true, PREFIX_DRIVER_EXECUTE_PIPELINE));
        List<Future<IPipelineResult>> list = new ArrayList<>(driverNum);
        int pipelineIndex = 0;
        for (Map.Entry<String, ConnectAddress> entry : driverAddresses.entrySet()) {
            list.add(executorService.submit(new ExecutePipelineTask(driverNum, pipelineIndex,
                pipeline, entry.getKey())));
            pipelineIndex++;
        }

        try {
            return list.get(0).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("submit pipeline failed", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public boolean isSync() {
        return false;
    }

    @Override
    public void close() {
        if (executorService != null) {
            ExecutorUtil.shutdown(executorService);
        }
    }

    private class ExecutePipelineTask implements Callable<IPipelineResult> {

        private final String driverId;
        private final Pipeline pipeline;
        private final int total;
        private final int index;

        private ExecutePipelineTask(int total, int index, Pipeline pipeline, String driverId) {
            this.driverId = driverId;
            this.pipeline = pipeline;
            this.total = total;
            this.index = index;
        }

        @Override
        public IPipelineResult call() throws Exception {
            int num = this.index + 1;
            LOGGER.info("execute pipeline [{}/{}]", num, this.total);
            long start = System.currentTimeMillis();
            IPipelineResult future = RpcClient.getInstance().executePipeline(driverId, pipeline);
            LOGGER.info("execute pipeline [{}/{}] costs {}ms, driver: {}", num, this.total,
                System.currentTimeMillis() - start, driverId);
            return future;
        }

    }
}
