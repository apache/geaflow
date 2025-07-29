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

package org.apache.geaflow.store.context;

import com.google.common.base.Preconditions;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.state.schema.GraphDataSchema;
import org.apache.geaflow.state.serializer.IKeySerializer;

public class StoreContext {

    private String name;
    private Configuration config;
    private MetricGroup metricGroup;

    private int shardId;
    private long version;
    private GraphDataSchema graphSchema;
    private IKeySerializer keySerializer;

    public StoreContext(String name) {
        this.name = name;
    }

    public StoreContext withConfig(Configuration config) {
        this.config = config;
        return this;
    }

    public StoreContext withShardId(int shardId) {
        this.shardId = shardId;
        return this;
    }

    public StoreContext withMetricGroup(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    public StoreContext withVersion(long version) {
        this.version = version;
        return this;
    }

    public StoreContext withName(String name) {
        this.name = name;
        return this;
    }

    public StoreContext withDataSchema(GraphDataSchema dataSchema) {
        this.graphSchema = dataSchema;
        return this;
    }

    public StoreContext withKeySerializer(IKeySerializer keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public Configuration getConfig() {
        return config;
    }

    public int getShardId() {
        return shardId;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public long getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public GraphDataSchema getGraphSchema() {
        return Preconditions.checkNotNull(graphSchema, "GraphMeta must be set");
    }

    public IKeySerializer getKeySerializer() {
        return keySerializer;
    }
}
