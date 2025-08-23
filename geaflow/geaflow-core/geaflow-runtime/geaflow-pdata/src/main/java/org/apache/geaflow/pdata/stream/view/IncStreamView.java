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

import org.apache.geaflow.api.function.base.AggregateFunction;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.base.ReduceFunction;
import org.apache.geaflow.api.pdata.stream.view.PIncStreamView;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.common.encoder.EncoderResolver;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.operator.impl.window.incremental.IncrAggregateOperator;
import org.apache.geaflow.operator.impl.window.incremental.IncrReduceOperator;
import org.apache.geaflow.pdata.stream.TransformType;
import org.apache.geaflow.pdata.stream.view.compute.ComputeIncStream;
import org.apache.geaflow.pipeline.context.IPipelineContext;

public class IncStreamView<KEY, T, R> extends AbstractStreamView<T, R> implements PIncStreamView<T> {

    protected KeySelector<T, KEY> keySelector;

    public IncStreamView(IPipelineContext pipelineContext, KeySelector<T, KEY> keySelector) {
        super(pipelineContext);
        this.keySelector = keySelector;
    }

    @Override
    public PWindowStream<T> reduce(ReduceFunction<T> reduceFunction) {
        IncrReduceOperator incrReduceOperator = new IncrReduceOperator(reduceFunction, keySelector);
        return new ComputeIncStream(pipelineContext, incrWindowStream, incrReduceOperator);
    }

    @Override
    public <ACC, OUT> PWindowStream<OUT> aggregate(
        AggregateFunction<T, ACC, OUT> aggregateFunction) {
        IncrAggregateOperator incrAggregateOperator = new IncrAggregateOperator(aggregateFunction, keySelector);
        IEncoder<?> resultEncoder = EncoderResolver.resolveFunction(AggregateFunction.class, aggregateFunction, 2);
        return new ComputeIncStream(pipelineContext, incrWindowStream, incrAggregateOperator).withEncoder(resultEncoder);
    }

    public PIncStreamView<T> withKeySelector(KeySelector<T, KEY> keySelector) {
        this.keySelector = keySelector;
        return this;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.ContinueStreamCompute;
    }
}
