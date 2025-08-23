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

package org.apache.geaflow.plan.graph;

public enum VertexType {
    /**
     * Source vertex.
     */
    source,
    /**
     * Process vertex.
     */
    process,
    /**
     * Incremental process vertex.
     */
    inc_process,
    /**
     * Combine vertex.
     */
    combine,
    /**
     * Join vertex.
     */
    join,
    /**
     * Union vertex.
     */
    union,
    /**
     * Partition vertex.
     */
    partition,
    /**
     * Vertex centric vertex.
     */
    vertex_centric,
    /**
     * Incremental Vertex centric vertex.
     */
    inc_vertex_centric,
    /**
     * Iterator vertex.
     */
    iterator,
    /**
     * Incremental Iterator vertex.
     */
    inc_iterator,
    /**
     * Sink vertex.
     */
    sink,
    /**
     * Collect vertex.
     */
    collect,
    /**
     * Iteration aggregation vertex.
     */
    iteration_aggregation
}
