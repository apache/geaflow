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

package org.apache.geaflow.pdata.stream.view;

import org.apache.geaflow.api.pdata.stream.view.PIncStreamView;
import org.apache.geaflow.api.pdata.stream.view.PStreamView;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.pdata.stream.window.WindowDataStream;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.stream.StreamViewDesc;

public abstract class AbstractStreamView<T, R> extends WindowDataStream<R> implements PStreamView<T> {

    protected IPipelineContext pipelineContext;
    protected StreamViewDesc streamViewDesc;
    protected PWindowStream<T> incrWindowStream;

    public AbstractStreamView(IPipelineContext pipelineContext) {
        this.pipelineContext = pipelineContext;
    }

    public AbstractStreamView(IPipelineContext pipelineContext, PWindowStream input, Operator operator) {
        super(pipelineContext, input, operator);
        this.pipelineContext = pipelineContext;
    }

    @Override
    public PStreamView<T> init(IViewDesc viewDesc) {
        this.streamViewDesc = (StreamViewDesc) viewDesc;
        return this;
    }

    @Override
    public PIncStreamView<T> append(PWindowStream<T> windowStream) {
        this.incrWindowStream = windowStream;
        return (PIncStreamView<T>) this;
    }
}
