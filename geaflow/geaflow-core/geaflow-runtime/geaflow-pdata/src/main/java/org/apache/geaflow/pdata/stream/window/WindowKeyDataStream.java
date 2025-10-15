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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.geaflow.api.function.base.AggregateFunction;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.base.ReduceFunction;
import org.apache.geaflow.api.pdata.stream.view.PIncStreamView;
import org.apache.geaflow.api.pdata.stream.window.PWindowKeyStream;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.encoder.EncoderResolver;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.context.AbstractPipelineContext;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.window.WindowAggregateOperator;
import org.apache.geaflow.operator.impl.window.WindowReduceOperator;
import org.apache.geaflow.partitioner.IPartitioner;
import org.apache.geaflow.partitioner.impl.KeyPartitioner;
import org.apache.geaflow.pdata.stream.view.IncStreamView;
import org.apache.geaflow.pipeline.context.IPipelineContext;

public class WindowKeyDataStream<KEY, T> extends WindowDataStream<T> implements
    PWindowKeyStream<KEY, T> {

    private KeySelector<T, KEY> keySelector;
    private boolean materializeDisable;

    public WindowKeyDataStream(IPipelineContext context, WindowDataStream dataStream,
                               AbstractOperator operator,
                               KeySelector<T, KEY> keySelector) {
        super(context, dataStream, operator);
        this.keySelector = keySelector;
        this.materializeDisable = ((AbstractPipelineContext) context).getConfig()
            .getBoolean(FrameworkConfigKeys.INC_STREAM_MATERIALIZE_DISABLE);
    }

    @Override
    public <ACC, OUT> PWindowStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction) {
        if (!materializeDisable) {
            return materialize().aggregate(aggregateFunction);
        }
        Preconditions.checkArgument(aggregateFunction != null, " aggregate Function must not be null");
        IEncoder<?> resultEncoder = EncoderResolver.resolveFunction(AggregateFunction.class, aggregateFunction, 2);
        return new WindowDataStream(this.context, this, new WindowAggregateOperator<>(aggregateFunction, keySelector)).withEncoder(resultEncoder);
    }

    @Override
    public PWindowStream<T> reduce(ReduceFunction<T> reduceFunction) {
        if (!materializeDisable) {
            return materialize().reduce(reduceFunction);
        }
        Preconditions.checkArgument(reduceFunction != null, " Reduce Function must not be null");
        return new WindowDataStream(this.context, this, new WindowReduceOperator<>(reduceFunction, keySelector)).withEncoder(this.encoder);
    }

    @Override
    public PIncStreamView<T> materialize() {
        IncStreamView incStreamView = new IncStreamView<>(context, keySelector);
        return incStreamView.append(this);
    }

    @Override
    public PWindowKeyStream<KEY, T> withConfig(Map config) {
        this.opArgs.setConfig(config);
        return this;
    }

    @Override
    public PWindowKeyStream<KEY, T> withConfig(String key, String value) {
        this.opArgs.getConfig().put(key, value);
        return this;
    }

    @Override
    public PWindowKeyStream<KEY, T> withName(String name) {
        setName(name);
        return this;
    }

    @Override
    public PWindowKeyStream<KEY, T> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    @Override
    public IPartitioner<T> getPartition() {
        return new KeyPartitioner(this.getId());
    }

    @Override
    public WindowKeyDataStream<KEY, T> withEncoder(IEncoder<T> encoder) {
        this.encoder = encoder;
        return this;
    }

}
