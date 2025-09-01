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

package org.apache.geaflow.shuffle.service;

import org.apache.geaflow.shuffle.api.reader.IShuffleReader;
import org.apache.geaflow.shuffle.api.reader.PipelineReader;
import org.apache.geaflow.shuffle.api.writer.IShuffleWriter;
import org.apache.geaflow.shuffle.api.writer.PipelineWriter;
import org.apache.geaflow.shuffle.message.PipelineInfo;
import org.apache.geaflow.shuffle.message.Shard;
import org.apache.geaflow.shuffle.network.IConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyShuffleService implements IShuffleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyShuffleService.class);

    private IConnectionManager connectionManager;

    @Override
    public void init(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public IShuffleReader getReader() {
        return new PipelineReader(this.connectionManager);
    }

    @Override
    public IShuffleWriter<?, Shard> getWriter() {
        return new PipelineWriter<>(this.connectionManager);
    }

    @Override
    public void clean(PipelineInfo jobInfo) {
        LOGGER.info("release shuffle data of job {}", jobInfo);
        ShuffleManager.getInstance().release(jobInfo.getPipelineId());
    }
}
