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

package org.apache.geaflow.dsl.connector.hudi;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.function.FunctionWrapper.throwingFlatMapWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

public class GeaFlowEngineContext extends HoodieEngineContext {

    private GeaFlowEngineContext(SerializableConfiguration conf,
                                 TaskContextSupplier taskContextSupplier) {
        super(conf, taskContextSupplier);
    }

    public static GeaFlowEngineContext create(RuntimeContext context, Configuration tableConf) {
        SerializableConfiguration hadoopConf =
            new SerializableConfiguration(FileConnectorUtil.toHadoopConf(tableConf));
        TaskContextSupplier taskContextSupplier = new GeaFlowTaskContextSupplier(context);
        return new GeaFlowEngineContext(hadoopConf, taskContextSupplier);
    }

    @Override
    public HoodieAccumulator newAccumulator() {
        return HoodieAtomicLongAccumulator.create();
    }

    @Override
    public <T> HoodieData<T> emptyHoodieData() {
        return HoodieListData.eager(Collections.emptyList());
    }

    @Override
    public <T> HoodieData<T> parallelize(List<T> list, int i) {
        return HoodieListData.eager(list);
    }

    @Override
    public <I, O> List<O> map(List<I> list, SerializableFunction<I, O> func, int i) {
        return list.stream().parallel().map(throwingMapWrapper(func)).collect(toList());
    }

    @Override
    public <I, K, V> List<V> mapToPairAndReduceByKey(List<I> list,
                                                     SerializablePairFunction<I, K, V> serializablePairFunction,
                                                     SerializableBiFunction<V, V, V> serializableBiFunction, int i) {
        throw new GeaFlowDSLException("Write hudi is not support");
    }

    @Override
    public <I, K, V> Stream<ImmutablePair<K, V>> mapPartitionsToPairAndReduceByKey(Stream<I> stream,
                                                                                   SerializablePairFlatMapFunction<Iterator<I>, K, V> serializablePairFlatMapFunction,
                                                                                   SerializableBiFunction<V, V, V> serializableBiFunction,
                                                                                   int i) {
        throw new GeaFlowDSLException("Write hudi is not support");
    }

    @Override
    public <I, K, V> List<V> reduceByKey(List<Pair<K, V>> list, SerializableBiFunction<V, V, V> serializableBiFunction,
                                         int i) {
        throw new GeaFlowDSLException("Write hudi is not support");
    }

    @Override
    public <I, O> List<O> flatMap(List<I> list, SerializableFunction<I, Stream<O>> func, int i) {
        return list.stream().parallel().flatMap(throwingFlatMapWrapper(func)).collect(toList());
    }

    @Override
    public <I> void foreach(List<I> list, SerializableConsumer<I> serializableConsumer, int i) {
        throw new GeaFlowDSLException("Write hudi is not support");
    }

    @Override
    public <I, K, V> Map<K, V> mapToPair(List<I> list, SerializablePairFunction<I, K, V> serializablePairFunction,
                                         Integer integer) {
        throw new GeaFlowDSLException("Write hudi is not support");
    }

    @Override
    public void setProperty(EngineProperty engineProperty, String s) {

    }

    @Override
    public Option<String> getProperty(EngineProperty engineProperty) {
        return Option.empty();
    }

    @Override
    public void setJobStatus(String s, String s1) {

    }
}
