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

package org.apache.geaflow.state.pushdown.filter.inner;

import java.util.List;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.pushdown.limit.IEdgeLimit;

public class SingleLimitFilter extends LimitFilter {

    private List<IGraphFilter> filters;
    private final long[] outCounters;
    private final long[] inCounters;
    private long needHitMaxVersion;
    private long hitMaxVersion;

    SingleLimitFilter(IGraphFilter filter, IEdgeLimit limit) {
        super(filter, limit);
        this.filters = ((OrGraphFilter) filter).childrenFilters;
        this.outCounters = new long[filters.size()];
        this.inCounters = new long[filters.size()];
        this.needHitMaxVersion = filters.size() * (inCounter + outCounter);
        if (this.needHitMaxVersion < 0) {
            this.needHitMaxVersion = Long.MAX_VALUE;
        }
    }

    @Override
    public boolean filterEdge(IEdge edge) {
        boolean keep = false;
        int i = 0;
        for (IGraphFilter filter : filters) {
            if (filter.filterEdge(edge)) {
                if (edge.getDirect() == EdgeDirection.OUT && outCounters[i] < outCounter) {
                    outCounters[i]++;
                    hitMaxVersion++;
                    keep = true;
                }
                if (edge.getDirect() == EdgeDirection.IN && inCounters[i] < inCounter) {
                    inCounters[i]++;
                    hitMaxVersion++;
                    keep = true;
                }
            }
            i++;
        }
        return keep;
    }

    @Override
    public boolean dropAllRemaining() {
        return hitMaxVersion >= needHitMaxVersion;
    }

    @Override
    public String toString() {
        return String.format("%s(%d, %d)", getClass().getSimpleName(), inCounter, outCounter);
    }
}
