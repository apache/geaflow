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

package org.apache.geaflow.ai.graph;

import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.ai.graph.io.GraphSchema;

public interface GraphAccessor {

    /**
     * Returned by {@link #getGraphVersion()} when the accessor cannot report content changes.
     * Callers must then treat every read as potentially different and skip caching.
     */
    long VERSION_UNSUPPORTED = -1L;

    /**
     * A monotonically increasing counter bumped on every content or schema change of the underlying
     * graph. Derived structures (verbalization caches, keyword indexes) compare it to decide whether
     * they are still valid, so that direct mutations of the graph cannot silently go unnoticed.
     *
     * @return current graph version, or {@link #VERSION_UNSUPPORTED} if change tracking is not
     *     available for this accessor
     */
    default long getGraphVersion() {
        return VERSION_UNSUPPORTED;
    }

    /**
     * Like {@link #getGraphVersion()} but only advanced by vertex and schema changes. Structures
     * derived from vertices alone can watch this and survive edge writes.
     *
     * @return current vertex version, defaults to {@link #getGraphVersion()}
     */
    default long getVertexVersion() {
        return getGraphVersion();
    }

    GraphSchema getGraphSchema();

    GraphVertex getVertex(String label, String id);

    List<GraphEdge> getEdge(String label, String src, String dst);

    Iterator<GraphVertex> scanVertex();

    Iterator<GraphEdge> scanEdge(GraphVertex vertex);

    List<GraphEntity> expand(GraphEntity entity);

    GraphAccessor copy();

    String getType();
}
