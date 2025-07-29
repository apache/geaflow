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
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.encoder.EncoderResolver;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.operator.impl.io.WindowSourceOperator;
import org.apache.geaflow.pdata.stream.TransformType;
import org.apache.geaflow.pipeline.context.IPipelineContext;

public class WindowStreamSource<OUT> extends WindowDataStream<OUT> implements PWindowSource<OUT> {

    protected IWindow<OUT> windowFunction;

    public WindowStreamSource(IPipelineContext pipelineContext,
                              SourceFunction<OUT> sourceFunction,
                              IWindow<OUT> windowFunction) {
        super(pipelineContext, new WindowSourceOperator<>(sourceFunction, windowFunction));
        this.windowFunction = windowFunction;
        this.encoder = (IEncoder<OUT>) EncoderResolver.resolveFunction(SourceFunction.class, sourceFunction);
    }

    @Override
    public PWindowSource<OUT> build(IPipelineContext pipelineContext,
                                    SourceFunction<OUT> sourceFunction,
                                    IWindow<OUT> window) {
        return new WindowStreamSource<>(pipelineContext, sourceFunction, window);
    }

    @Override
    public WindowStreamSource<OUT> window(IWindow<OUT> window) {
        this.windowFunction = window;
        return this;
    }


    @Override
    public WindowStreamSource<OUT> withConfig(Map map) {
        super.withConfig(map);
        return this;
    }

    @Override
    public WindowStreamSource<OUT> withConfig(String key, String value) {
        super.withConfig(key, value);
        return this;
    }

    @Override
    public WindowStreamSource<OUT> withName(String name) {
        super.withName(name);
        return this;
    }

    @Override
    public WindowStreamSource<OUT> withParallelism(int parallelism) {
        super.withParallelism(parallelism);
        return this;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.StreamSource;
    }

    @Override
    public WindowStreamSource<OUT> withEncoder(IEncoder<OUT> encoder) {
        this.encoder = encoder;
        return this;
    }

}
