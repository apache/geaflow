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

import java.io.Serializable;
import org.apache.geaflow.shuffle.api.reader.IShuffleReader;
import org.apache.geaflow.shuffle.api.writer.IShuffleWriter;
import org.apache.geaflow.shuffle.message.PipelineInfo;
import org.apache.geaflow.shuffle.message.Shard;
import org.apache.geaflow.shuffle.network.IConnectionManager;

public interface IShuffleService extends Serializable {

    /**
     * Init the shuffle service with job config.
     *
     * @param connectionManager connection manager.
     */
    void init(IConnectionManager connectionManager);

    /**
     * get shuffle writer per reducer task.
     *
     * @return shuffle reader.
     */
    IShuffleReader getReader();

    /**
     * get shuffle reader per mapper task.
     *
     * @return shuffle writer.
     */
    IShuffleWriter<?, Shard> getWriter();

    /**
     * Release the local resources of this job id.
     */
    void clean(PipelineInfo jobInfo);
}
