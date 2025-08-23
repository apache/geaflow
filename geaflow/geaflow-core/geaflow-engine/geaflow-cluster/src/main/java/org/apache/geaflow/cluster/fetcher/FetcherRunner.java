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

package org.apache.geaflow.cluster.fetcher;

import org.apache.geaflow.cluster.task.runner.AbstractTaskRunner;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherRunner extends AbstractTaskRunner<IFetchRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetcherRunner.class);

    private final PipelineInputFetcher fetcher;

    public FetcherRunner(Configuration configuration) {
        this.fetcher = new PipelineInputFetcher(configuration);
    }

    @Override
    protected void process(IFetchRequest task) {
        IFetchRequest.RequestType requestType = task.getRequestType();
        switch (requestType) {
            case INIT:
                this.fetcher.init((InitFetchRequest) task);
                break;
            case FETCH:
                this.fetcher.fetch((FetchRequest) task);
                break;
            case CLOSE:
                this.fetcher.close((CloseFetchRequest) task);
                break;
            default:
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.requestTypeNotSupportError(requestType.name()));
        }
    }

    @Override
    public void interrupt() {
        LOGGER.info("cancel fetcher runner");
        fetcher.cancel();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        fetcher.close();
    }

}
