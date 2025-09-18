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

import static org.apache.geaflow.store.paimon.config.PaimonConfigKeys.PAIMON_STORE_DATABASE;
import static org.apache.geaflow.store.paimon.config.PaimonConfigKeys.PAIMON_STORE_EDGE_TABLE;
import static org.apache.geaflow.store.paimon.config.PaimonConfigKeys.PAIMON_STORE_INDEX_TABLE;
import static org.apache.geaflow.store.paimon.config.PaimonConfigKeys.PAIMON_STORE_VERTEX_TABLE;

import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.pushdown.inner.CodeGenFilterConverter;
import org.apache.geaflow.state.pushdown.inner.DirectFilterConverter;
import org.apache.geaflow.state.pushdown.inner.IFilterConverter;
import org.apache.geaflow.store.api.graph.IPushDownStore;
import org.apache.geaflow.store.config.StoreConfigKeys;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.paimon.catalog.Identifier;

public abstract class BasePaimonGraphStore extends BasePaimonStore implements IPushDownStore {

    protected IFilterConverter filterConverter;
    protected String vertexTable;
    protected String edgeTable;
    protected String indexTable;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        Configuration config = storeContext.getConfig();
        boolean codegenEnable = config.getBoolean(StoreConfigKeys.STORE_FILTER_CODEGEN_ENABLE);
        filterConverter =
            codegenEnable ? new CodeGenFilterConverter() : new DirectFilterConverter();
        // use distributed table when distributed mode or database is configured.
        if (config.contains(PAIMON_STORE_DATABASE) || isDistributedMode) {
            paimonStoreName = config.getString(PAIMON_STORE_DATABASE);
            vertexTable = config.getString(PAIMON_STORE_VERTEX_TABLE);
            edgeTable = config.getString(PAIMON_STORE_EDGE_TABLE);
            indexTable = config.getString(PAIMON_STORE_INDEX_TABLE);
        }
    }

    @Override
    public IFilterConverter getFilterConverter() {
        return filterConverter;
    }

    protected PaimonTableRWHandle createVertexTable(int shardId) {
        String tableName = vertexTable;
        if (StringUtils.isEmpty(tableName)) {
            tableName = String.format("%s#%s", "vertex", shardId);
        }
        return createTable(tableName);
    }

    protected PaimonTableRWHandle createEdgeTable(int shardId) {
        String tableName = edgeTable;
        if (StringUtils.isEmpty(edgeTable)) {
            tableName = String.format("%s#%s", "edge", shardId);
        }
        return createTable(tableName);
    }

    protected PaimonTableRWHandle createIndexTable(int shardId) {
        String tableName = indexTable;
        if (StringUtils.isEmpty(indexTable)) {
            tableName = String.format("%s#%s", "vertex_index", shardId);
        }
        return createTable(tableName);
    }

    private PaimonTableRWHandle createTable(String tableName) {
        Identifier vertexIndexIdentifier = new Identifier(paimonStoreName, tableName);
        return createKVTableHandle(vertexIndexIdentifier);
    }
}
