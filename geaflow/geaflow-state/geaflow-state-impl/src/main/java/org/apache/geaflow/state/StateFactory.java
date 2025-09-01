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

package org.apache.geaflow.state;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.descriptor.KeyListStateDescriptor;
import org.apache.geaflow.state.descriptor.KeyMapStateDescriptor;
import org.apache.geaflow.state.descriptor.KeyValueStateDescriptor;
import org.apache.geaflow.state.serializer.DefaultKMapSerializer;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;

public class StateFactory implements Serializable {

    private static final long serialVersionUID = 6070809556097701233L;

    private static final List<StoreType> SUPPORTED_KEY_STORE_TYPES =
        Arrays.asList(StoreType.ROCKSDB, StoreType.MEMORY, StoreType.PAIMON);

    private static final Map<String, GraphState> GRAPH_STATE_MAP = new ConcurrentHashMap<>();

    public static <K, VV, EV> GraphState<K, VV, EV> buildGraphState(
        GraphStateDescriptor<K, VV, EV> descriptor, Configuration configuration) {
        if (descriptor.isSingleton()) {
            return GRAPH_STATE_MAP.computeIfAbsent(descriptor.getName(),
                k -> getGraphState(descriptor, configuration));
        }
        return getGraphState(descriptor, configuration);
    }

    private static <K, VV, EV> GraphState<K, VV, EV> getGraphState(
        GraphStateDescriptor<K, VV, EV> descriptor, Configuration configuration) {
        if (descriptor.getDateModel() == null) {
            descriptor.withDataModel(DataModel.STATIC_GRAPH);
        }
        return new GraphStateImpl<>(new StateContext(descriptor, configuration));
    }


    public static <K, V> KeyValueState<K, V> buildKeyValueState(
        KeyValueStateDescriptor<K, V> descriptor, Configuration configuration) {

        Preconditions.checkArgument(SUPPORTED_KEY_STORE_TYPES.contains(
                StoreType.getEnum(descriptor.getStoreType())),
            "only support %s", SUPPORTED_KEY_STORE_TYPES);
        if (descriptor.getKeySerializer() == null) {
            descriptor.withKVSerializer(new DefaultKVSerializer<>(
                descriptor.getKeyClazz(), descriptor.getValueClazz()));
        }
        descriptor.withDataModel(DataModel.KV);
        return new KeyValueStateImpl<>(new StateContext(descriptor, configuration));
    }

    public static <K, V> KeyListState<K, V> buildKeyListState(
        KeyListStateDescriptor<K, V> descriptor, Configuration configuration) {
        Preconditions.checkArgument(SUPPORTED_KEY_STORE_TYPES.contains(
                StoreType.getEnum(descriptor.getStoreType())),
            "only support %s", SUPPORTED_KEY_STORE_TYPES);
        String storeType = descriptor.getStoreType();
        IStoreBuilder builder = StoreBuilderFactory.build(storeType);
        if (builder.supportedDataModel().contains(DataModel.KList)) {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(new DefaultKVSerializer<>(descriptor.getKeyClazz(),
                    descriptor.getValueClazz()));
            }
            descriptor.withDataModel(DataModel.KList);
            return new KeyListStateImpl<>(new StateContext(descriptor, configuration), descriptor.getValueClazz());
        } else {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(new DefaultKVSerializer<>(descriptor.getKeyClazz(), List.class));
            }
            descriptor.withDataModel(DataModel.KV);
            return new KeyValueListStateImpl<>(new StateContext(descriptor, configuration));
        }

    }

    public static <K, UK, UV> KeyMapState<K, UK, UV> buildKeyMapState(
        KeyMapStateDescriptor<K, UK, UV> descriptor, Configuration configuration) {
        Preconditions.checkArgument(SUPPORTED_KEY_STORE_TYPES.contains(
                StoreType.getEnum(descriptor.getStoreType())),
            "only support %s", SUPPORTED_KEY_STORE_TYPES);
        String storeType = descriptor.getStoreType();
        IStoreBuilder builder = StoreBuilderFactory.build(storeType);
        if (builder.supportedDataModel().contains(DataModel.KMap)) {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(new DefaultKMapSerializer<>(descriptor.getKeyClazz(),
                    descriptor.getSubKeyClazz(), descriptor.getValueClazz()));
            }
            descriptor.withDataModel(DataModel.KMap);
            return new KeyMapStateImpl<>(new StateContext(descriptor, configuration),
                descriptor.getSubKeyClazz(), descriptor.getValueClazz());
        } else {
            if (descriptor.getKeySerializer() == null) {
                descriptor.withKeySerializer(
                    new DefaultKVSerializer<>(descriptor.getKeyClazz(), Map.class));
            }
            descriptor.withDataModel(DataModel.KV);
            return new KeyValueMapStateImpl<>(new StateContext(descriptor, configuration));
        }
    }
}
