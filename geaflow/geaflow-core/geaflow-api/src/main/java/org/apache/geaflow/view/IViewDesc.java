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

package org.apache.geaflow.view;

import java.io.Serializable;
import java.util.Map;

public interface IViewDesc extends Serializable {

    /**
     * Returns the view name.
     */
    String getName();

    /**
     * Returns the shard num of view.
     */
    int getShardNum();

    /**
     * Returns the data model.
     */
    DataModel getDataModel();

    /**
     * Returns the backend type.
     */
    BackendType getBackend();

    /**
     * Returns the view properties.
     */
    Map getViewProps();


    enum DataModel {
        // Table data model.
        TABLE,
        // Graph data model.
        GRAPH,
    }

    enum BackendType {
        // Default view backend, current is pangu.
        Native,
        // RocksDB backend.
        RocksDB,
        // Memory backend.
        Memory,
        // Paimon backend.
        Paimon,
        // Custom backend.
        Custom;

        public static BackendType of(String type) {
            for (BackendType value : values()) {
                if (value.name().equalsIgnoreCase(type)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Illegal backend type: " + type);
        }
    }

}
