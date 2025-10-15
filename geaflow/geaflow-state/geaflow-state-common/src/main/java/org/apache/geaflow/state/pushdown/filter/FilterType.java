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

package org.apache.geaflow.state.pushdown.filter;

import org.apache.geaflow.state.pushdown.inner.PushDownPb;

public enum FilterType {
    /**
     * empty filter.
     */
    EMPTY(false),
    /**
     * in edge filter.
     */
    IN_EDGE(false),
    /**
     * out edge filter.
     */
    OUT_EDGE(false),
    /**
     * edge ts filter.
     */
    EDGE_TS(false),
    /**
     * ttl filter.
     */
    TTL(false),
    /**
     * logic or filters.
     */
    OR(false),
    /**
     * logic and filters.
     */
    AND(false),
    /**
     * only fetch vertex.
     */
    ONLY_VERTEX(true),
    /**
     * vertex ts filter.
     */
    VERTEX_TS(false),
    /**
     * edge value drop.
     */
    EDGE_VALUE_DROP(true),
    /**
     * vertex value drop.
     */
    VERTEX_VALUE_DROP(true),
    /**
     * edge label filter.
     */
    EDGE_LABEL(false),
    /**
     * vertex label filter.
     */
    VERTEX_LABEL(false),
    /**
     * result must contain vertex.
     */
    VERTEX_MUST_CONTAIN(true),

    /**
     * generated filter.
     */
    GENERATED(false),
    /**
     * other filter type.
     */
    OTHER(false);

    private final PushDownPb.FilterType pbFilterType;
    private boolean isRootFilter;

    FilterType(boolean isRootFilter) {
        this.isRootFilter = isRootFilter;
        this.pbFilterType = PushDownPb.FilterType.valueOf(this.name());
    }

    public boolean isRootFilter() {
        return isRootFilter;
    }

    public PushDownPb.FilterType toPbFilterType() {
        return pbFilterType;
    }
}
