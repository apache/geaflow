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

package org.apache.geaflow.pdata.stream.window;

import java.util.Map;
import org.apache.geaflow.api.pdata.stream.window.PWindowBroadcastStream;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.pipeline.context.IPipelineContext;

public class WindowBroadcastDataStream<T> extends WindowDataStream<T> implements PWindowBroadcastStream<T> {

    public WindowBroadcastDataStream(IPipelineContext pipelineContext, PWindowStream<T> input,
                                     Operator operator) {
        super(pipelineContext, input, operator);
    }

    @Override
    public PWindowBroadcastStream<T> withConfig(Map config) {
        setConfig(config);
        return this;
    }

    @Override
    public PWindowBroadcastStream<T> withConfig(String key, String value) {
        setConfig(key, value);
        return this;
    }

    @Override
    public PWindowBroadcastStream<T> withName(String name) {
        this.opArgs.setOpName(name);
        return this;
    }

    @Override
    public PWindowBroadcastStream<T> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    @Override
    public WindowBroadcastDataStream<T> withEncoder(IEncoder<T> encoder) {
        this.encoder = encoder;
        return this;
    }
}
