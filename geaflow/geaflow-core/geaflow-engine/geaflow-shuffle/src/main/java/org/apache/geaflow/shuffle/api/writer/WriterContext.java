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

import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.shuffle.DataExchangeMode;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.PipelineInfo;

public class WriterContext implements IWriterContext {

    private final PipelineInfo pipelineInfo;
    private int edgeId;
    private int vertexId;
    private int taskIndex;
    private int taskId;
    private String taskName;
    private DataExchangeMode dataExchangeMode;
    private int targetChannels;
    private ShuffleConfig config;
    private IEncoder<?> encoder;

    public WriterContext(long pipelineId, String pipelineName) {
        this.pipelineInfo = new PipelineInfo(pipelineId, pipelineName);
    }

    public WriterContext setEdgeId(int edgeId) {
        this.edgeId = edgeId;
        return this;
    }

    public WriterContext setVertexId(int vertexId) {
        this.vertexId = vertexId;
        return this;
    }

    public WriterContext setTaskIndex(int taskIndex) {
        this.taskIndex = taskIndex;
        return this;
    }

    public WriterContext setTaskId(int taskId) {
        this.taskId = taskId;
        return this;
    }

    public WriterContext setTaskName(String taskName) {
        this.taskName = taskName;
        return this;
    }

    public WriterContext setDataExchangeMode(DataExchangeMode dataExchangeMode) {
        this.dataExchangeMode = dataExchangeMode;
        return this;
    }

    public WriterContext setChannelNum(int targetChannels) {
        this.targetChannels = targetChannels;
        return this;
    }

    public WriterContext setConfig(ShuffleConfig config) {
        this.config = config;
        return this;
    }

    public WriterContext setEncoder(IEncoder<?> encoder) {
        this.encoder = encoder;
        return this;
    }

    @Override
    public PipelineInfo getPipelineInfo() {
        return pipelineInfo;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public int getVertexId() {
        return vertexId;
    }

    @Override
    public int getTaskId() {
        return taskId;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public int getTaskIndex() {
        return taskIndex;
    }

    @Override
    public ShuffleConfig getConfig() {
        return config;
    }

    @Override
    public int getTargetChannelNum() {
        return targetChannels;
    }

    @Override
    public IEncoder<?> getEncoder() {
        return this.encoder;
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return this.dataExchangeMode;
    }

    public static WriterContextBuilder newBuilder() {
        return new WriterContextBuilder();
    }

    public static class WriterContextBuilder {

        private long pipelineId;

        public WriterContextBuilder setPipelineId(long pipelineId) {
            this.pipelineId = pipelineId;
            return this;
        }

        public WriterContext setPipelineName(String pipelineName) {
            return new WriterContext(pipelineId, pipelineName);
        }
    }

}
