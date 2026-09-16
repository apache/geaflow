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

package org.apache.geaflow.ai.index;

import java.util.Collection;
import java.util.List;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.index.vector.IVector;

public interface IndexStore {

    List<IVector> getEntityIndex(GraphEntity entity);

    /**
     * Returns the entities this store actually holds an index for, or {@code null} when the store
     * cannot enumerate them (e.g. it derives the index on demand for any entity).
     *
     * <p>When available, callers can iterate this collection instead of scanning the whole graph:
     * entities absent from the store contribute nothing to recall anyway.
     *
     * @return indexed entities, or {@code null} if unknown
     */
    default Collection<GraphEntity> getIndexedEntities() {
        return null;
    }
}
