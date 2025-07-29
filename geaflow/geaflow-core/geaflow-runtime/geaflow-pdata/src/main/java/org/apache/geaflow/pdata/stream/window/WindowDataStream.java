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
import org.apache.geaflow.api.function.base.FilterFunction;
import org.apache.geaflow.api.function.base.FlatMapFunction;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.base.MapFunction;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.pdata.PWindowCollect;
import org.apache.geaflow.api.pdata.stream.PStream;
import org.apache.geaflow.api.pdata.stream.window.PWindowBroadcastStream;
import org.apache.geaflow.api.pdata.stream.window.PWindowKeyStream;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.common.encoder.EncoderResolver;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.impl.window.BroadcastOperator;
import org.apache.geaflow.operator.impl.window.CollectOperator;
import org.apache.geaflow.operator.impl.window.FilterOperator;
import org.apache.geaflow.operator.impl.window.FlatMapOperator;
import org.apache.geaflow.operator.impl.window.KeySelectorOperator;
import org.apache.geaflow.operator.impl.window.MapOperator;
import org.apache.geaflow.operator.impl.window.SinkOperator;
import org.apache.geaflow.operator.impl.window.UnionOperator;
import org.apache.geaflow.pdata.stream.Stream;
import org.apache.geaflow.pipeline.context.IPipelineContext;

public class WindowDataStream<T> extends Stream<T> implements PWindowStream<T> {


    public WindowDataStream() {
    }

    public WindowDataStream(Stream<T> input, Operator operator) {
        super(input, operator);
    }


    public WindowDataStream(IPipelineContext pipelineContext) {
        super(pipelineContext);
    }

    public WindowDataStream(IPipelineContext pipelineContext, Operator operator) {
        super(pipelineContext, operator);
    }

    public WindowDataStream(IPipelineContext pipelineContext, PWindowStream<T> input,
                            Operator operator) {
        this(pipelineContext, operator);
        this.input = (Stream) input;
        this.parallelism = input.getParallelism();
        this.opArgs.setParallelism(input.getParallelism());
    }

    @Override
    public <R> WindowDataStream<R> map(MapFunction<T, R> mapFunction) {
        Preconditions.checkArgument(mapFunction != null, " Map Function must not be null");
        IEncoder<?> resultEncoder = EncoderResolver.resolveFunction(MapFunction.class, mapFunction, 1);
        return new WindowDataStream(this.context, this, new MapOperator<>(mapFunction)).withEncoder(resultEncoder);
    }

    @Override
    public PWindowStream<T> filter(FilterFunction<T> filterFunction) {
        Preconditions.checkArgument(filterFunction != null, " Filter Function must not be null");
        return new WindowDataStream(this.context, this, new FilterOperator<>(filterFunction)).withEncoder(this.encoder);
    }

    @Override
    public <R> PWindowStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction) {
        Preconditions.checkArgument(flatMapFunction != null, " FlatMap Function must not be null");
        IEncoder<?> resultEncoder = EncoderResolver.resolveFunction(FlatMapFunction.class, flatMapFunction, 1);
        return new WindowDataStream(this.context, this, new FlatMapOperator(flatMapFunction)).withEncoder(resultEncoder);
    }

    @Override
    public PWindowStream<T> union(PStream<T> uStream) {
        if (this instanceof WindowUnionStream) {
            ((WindowUnionStream<T>) this).addUnionDataStream((WindowDataStream) uStream);
            return this;
        } else {
            return new WindowUnionStream(this, (WindowDataStream<T>) uStream,
                new UnionOperator()).withEncoder(this.encoder);
        }
    }

    @Override
    public PWindowBroadcastStream<T> broadcast() {
        return new WindowBroadcastDataStream(this.context, this, new BroadcastOperator()).withEncoder(encoder);
    }

    @Override
    public <KEY> PWindowKeyStream<KEY, T> keyBy(KeySelector<T, KEY> selectorFunction) {
        Preconditions.checkArgument(selectorFunction != null, " KeySelector Function must not be null");
        return new WindowKeyDataStream<KEY, T>(context, this,
            new KeySelectorOperator(selectorFunction), selectorFunction).withEncoder(this.encoder);
    }

    @Override
    public WindowStreamSink<T> sink(SinkFunction<T> sinkFunction) {
        Preconditions.checkArgument(sinkFunction != null, " Sink Function must not be null");
        WindowStreamSink sink = new WindowStreamSink(this, new SinkOperator<>(sinkFunction));
        context.addPAction(sink);
        return sink;
    }

    @Override
    public PWindowCollect<T> collect() {
        WindowStreamCollect<T> collect = new WindowStreamCollect<>(this, new CollectOperator());
        context.addPAction(collect);
        return collect;
    }

    @Override
    public PWindowStream<T> withConfig(Map config) {
        setConfig(config);
        return this;
    }

    @Override
    public PWindowStream<T> withConfig(String key, String value) {
        setConfig(key, value);
        return this;
    }

    @Override
    public PWindowStream<T> withName(String name) {
        this.opArgs.setOpName(name);
        return this;
    }

    @Override
    public PWindowStream<T> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    @Override
    public WindowDataStream<T> withEncoder(IEncoder<T> encoder) {
        this.encoder = encoder;
        return this;
    }

}
