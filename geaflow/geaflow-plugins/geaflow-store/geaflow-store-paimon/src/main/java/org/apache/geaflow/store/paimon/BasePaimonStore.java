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

package org.apache.geaflow.store.paimon;

import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.store.IStatefulStore;
import org.apache.geaflow.store.api.graph.BaseGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

public abstract class BasePaimonStore extends BaseGraphStore implements IStatefulStore {

    protected static final String KEY_COLUMN_NAME = "key";
    protected static final String VALUE_COLUMN_NAME = "value";
    protected static final int KEY_COLUMN_INDEX = 0;
    protected static final int VALUE_COLUMN_INDEX = 1;

    // 新增的常量定义
    protected static final String TARGET_ID_COLUMN_NAME = "target_id";
    protected static final String SRC_ID_COLUMN_NAME = "src_id";
    protected static final String TS_COLUMN_NAME = "ts";
    protected static final String DIRECTION_COLUMN_NAME = "direction";
    protected static final String LABEL_COLUMN_NAME = "label";

    protected PaimonTableCatalogClient client;
    protected int shardId;
    protected String jobName;
    protected String paimonStoreName;
    protected long lastCheckpointId;

    @Override
    public void init(StoreContext storeContext) {
        this.shardId = storeContext.getShardId();
        this.jobName = storeContext.getConfig().getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.paimonStoreName = this.jobName + "#" + this.shardId;
        this.client = new PaimonTableCatalogClient(storeContext.getConfig());
        this.lastCheckpointId = Long.MAX_VALUE;
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public void drop() {
        this.client.dropDatabase(paimonStoreName);
    }

    protected PaimonTableRWHandle createKVTableHandle(Identifier identifier) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey(KEY_COLUMN_NAME);
        schemaBuilder.column(KEY_COLUMN_NAME, DataTypes.BYTES());
        schemaBuilder.column(VALUE_COLUMN_NAME, DataTypes.BYTES());
        Schema schema = schemaBuilder.build();
        Table vertexTable = this.client.createTable(schema, identifier);
        return new PaimonTableRWHandle(identifier, vertexTable);
    }

    protected PaimonTableRWHandle createEdgeTableHandle(Identifier identifier) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey(SRC_ID_COLUMN_NAME);
        schemaBuilder.primaryKey(TARGET_ID_COLUMN_NAME);
        schemaBuilder.primaryKey(TS_COLUMN_NAME);
        schemaBuilder.primaryKey(DIRECTION_COLUMN_NAME);
        schemaBuilder.primaryKey(LABEL_COLUMN_NAME);
        schemaBuilder.column(SRC_ID_COLUMN_NAME, DataTypes.BYTES());
        schemaBuilder.column(TARGET_ID_COLUMN_NAME, DataTypes.BYTES());
        schemaBuilder.column(TS_COLUMN_NAME, DataTypes.BIGINT());
        schemaBuilder.column(DIRECTION_COLUMN_NAME, DataTypes.SMALLINT());
        schemaBuilder.column(LABEL_COLUMN_NAME, DataTypes.BYTES());
        schemaBuilder.column(VALUE_COLUMN_NAME, DataTypes.BYTES());
        Schema schema = schemaBuilder.build();
        Table vertexTable = this.client.createTable(schema, identifier);
        return new PaimonTableRWHandle(identifier, vertexTable);
    }

    protected PaimonTableRWHandle createVertexTableHandle(Identifier identifier) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey(SRC_ID_COLUMN_NAME);
        schemaBuilder.column(SRC_ID_COLUMN_NAME, DataTypes.BYTES());
        schemaBuilder.column(TS_COLUMN_NAME, DataTypes.BIGINT());
        schemaBuilder.column(LABEL_COLUMN_NAME, DataTypes.SMALLINT());
        schemaBuilder.column(VALUE_COLUMN_NAME, DataTypes.BYTES());
        Schema schema = schemaBuilder.build();
        Table vertexTable = this.client.createTable(schema, identifier);
        return new PaimonTableRWHandle(identifier, vertexTable);
    }
}
