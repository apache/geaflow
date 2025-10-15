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

package org.apache.geaflow.api.pdata.stream;

import java.util.Map;
import org.apache.geaflow.api.function.base.FilterFunction;
import org.apache.geaflow.api.function.base.FlatMapFunction;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.base.MapFunction;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.api.pdata.PStreamSink;
import org.apache.geaflow.api.pdata.base.PData;
import org.apache.geaflow.common.encoder.IEncoder;

public interface PStream<T> extends PData {

    <R> PStream<R> map(MapFunction<T, R> mapFunction);

    PStream<T> filter(FilterFunction<T> filterFunction);

    <R> PStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction);

    PStream<T> union(PStream<T> uStream);

    PStream<T> broadcast();

    <KEY> PKeyStream<KEY, T> keyBy(KeySelector<T, KEY> selectorFunction);

    PStreamSink<T> sink(SinkFunction<T> sinkFunction);

    @Override
    PStream<T> withConfig(Map map);

    @Override
    PStream<T> withConfig(String key, String value);

    @Override
    PStream<T> withName(String name);

    @Override
    PStream<T> withParallelism(int parallelism);


    PStream<T> withEncoder(IEncoder<T> encoder);

}
