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

package org.apache.geaflow.shuffle.api.writer;

import java.io.Serializable;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.shuffle.DataExchangeMode;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.PipelineInfo;

public interface IWriterContext extends Serializable {

    /**
     * Get the pipeline information.
     *
     * @return pipeline info.
     */
    PipelineInfo getPipelineInfo();

    /**
     * Get the vertex id in the DAG.
     *
     * @return vertex id.
     */
    int getVertexId();

    /**
     * Get the edge id in the DAG.
     *
     * @return edge id.
     */
    int getEdgeId();

    /**
     * Get the task index of the writer.
     *
     * @return task index.
     */
    int getTaskIndex();

    /**
     * Get the task id of the writer.
     *
     * @return task id.
     */
    int getTaskId();

    /**
     * Get task name of the writer.
     *
     * @return task name.
     */
    String getTaskName();

    /**
     * Get the target channel number of downstream.
     *
     * @return target channel number.
     */
    int getTargetChannelNum();

    /**
     * Get the configuration.
     *
     * @return configuration.
     */
    ShuffleConfig getConfig();

    /**
     * Get the encoder for serialize and deserialize data.
     *
     * @return data encoder.
     */
    IEncoder<?> getEncoder();

    DataExchangeMode getDataExchangeMode();

}
