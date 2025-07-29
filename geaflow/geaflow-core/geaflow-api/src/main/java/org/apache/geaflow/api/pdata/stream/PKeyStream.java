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
import org.apache.geaflow.api.function.base.AggregateFunction;
import org.apache.geaflow.api.function.base.ReduceFunction;
import org.apache.geaflow.common.encoder.IEncoder;

public interface PKeyStream<KEY, T> extends PStream<T> {

    /**
     * Default is incremental reduce compute, otherwise is window reduce compute if INC_STREAM_MATERIALIZE_DISABLE is true.
     */
    PStream<T> reduce(ReduceFunction<T> reduceFunction);

    /**
     * Default is incremental aggregate compute, otherwise is window aggregate compute if INC_STREAM_MATERIALIZE_DISABLE is true.
     */
    <ACC, OUT> PStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction);

    @Override
    PKeyStream<KEY, T> withConfig(Map map);

    @Override
    PKeyStream<KEY, T> withConfig(String key, String value);

    @Override
    PKeyStream<KEY, T> withName(String name);

    @Override
    PKeyStream<KEY, T> withParallelism(int parallelism);

    @Override
    PKeyStream<KEY, T> withEncoder(IEncoder<T> encoder);

}
