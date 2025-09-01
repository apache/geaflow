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

package org.apache.geaflow.dsl.catalog.console;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Field category, indicating what struct for {@link GeaFlowStructType} it belongs to.
 */
public enum GeaFlowFieldCategory {
    /**
     * Property, all data structures in {@link GeaFlowStructType} have it.
     */
    PROPERTY(GeaFlowStructType.values()),
    /**
     * Id, table struct and view struct have it.
     */
    ID(GeaFlowStructType.TABLE, GeaFlowStructType.VIEW),
    /**
     * Vertex id, only vertex struct has it.
     */
    VERTEX_ID(GeaFlowStructType.VERTEX),
    /**
     * Vertex label, only vertex struct has it.
     */
    VERTEX_LABEL(GeaFlowStructType.VERTEX),
    /**
     * Edge source id, only edge struct has it.
     */
    EDGE_SOURCE_ID(GeaFlowStructType.EDGE),
    /**
     * Edge target id, only edge struct has it.
     */
    EDGE_TARGET_ID(GeaFlowStructType.EDGE),
    /**
     * Edge label, only edge struct has it.
     */
    EDGE_LABEL(GeaFlowStructType.EDGE),
    /**
     * Edge timestamp, only edge struct has it.
     */
    EDGE_TIMESTAMP(GeaFlowStructType.EDGE);

    private final Set<GeaFlowStructType> structTypes;

    GeaFlowFieldCategory(GeaFlowStructType... structTypes) {
        this.structTypes = Sets.newHashSet(structTypes);
    }

    public static List<GeaFlowFieldCategory> of(GeaFlowStructType structType) {
        List<GeaFlowFieldCategory> constraints = new ArrayList<>();
        for (GeaFlowFieldCategory value : values()) {
            if (value.structTypes.contains(structType)) {
                constraints.add(value);
            }
        }
        return constraints;
    }
}
