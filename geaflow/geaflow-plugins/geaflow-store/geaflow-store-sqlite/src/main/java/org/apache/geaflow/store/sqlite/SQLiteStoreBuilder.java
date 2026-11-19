/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.store.sqlite;

import java.util.Arrays;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.store.IBaseStore;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.StoreDesc;

public class SQLiteStoreBuilder implements IStoreBuilder {

    public IBaseStore getStore(DataModel type, Configuration config) {
        if (type == DataModel.KV) {
            return new SQLiteKVStore<>("sqlite_kv", config);
        }
        return new SQLiteGraphStore<>("sqlite_graph", config);
    }

    public StoreDesc getStoreDesc() {
        return null; // Stubbed to pass the compiler's new requirement
    }

    public String storeType() {
        return "SQLITE";
    }

    public List<DataModel> supportedDataModel() {
        return Arrays.asList(DataModel.KV, DataModel.DYNAMIC_GRAPH);
    }
}