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

package org.apache.geaflow.model.graph.edge;

import java.io.Serializable;

public interface IEdge<K, EV> extends Serializable {

    /**
     * Get the source id of edge.
     *
     * @return source id
     */
    K getSrcId();

    /**
     * Set the source id for the edge.
     *
     * @param srcId source id
     */
    void setSrcId(K srcId);

    /**
     * Get the target id of edge.
     *
     * @return target id
     */
    K getTargetId();

    /**
     * Set the target id for the edge.
     *
     * @param targetId target id
     */
    void setTargetId(K targetId);

    /**
     * Get the direction of edge.
     *
     * @return direction
     */
    EdgeDirection getDirect();

    /**
     * Set the direction for the edge.
     *
     * @param direction direction
     */
    void setDirect(EdgeDirection direction);

    /**
     * Get the value of edge.
     *
     * @return value
     */
    EV getValue();

    /**
     * Reset the value for the edge.
     *
     * @param value value
     */
    IEdge<K, EV> withValue(EV value);

    /**
     * Reverse the source id and target id, and return new edge.
     *
     * @return edge
     */
    IEdge<K, EV> reverse();

}
