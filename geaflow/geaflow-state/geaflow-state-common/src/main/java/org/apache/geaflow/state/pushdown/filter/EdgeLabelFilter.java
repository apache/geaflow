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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Set;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.edge.IEdge;

public class EdgeLabelFilter<K, EV> implements IEdgeFilter<K, EV> {

    private Set<String> labels;

    public EdgeLabelFilter(Collection<String> list) {
        this.labels = Sets.newHashSet(list);
    }

    public EdgeLabelFilter(String... labels) {
        this.labels = Sets.newHashSet(labels);
    }

    public static <K, EV> EdgeLabelFilter<K, EV> getInstance(String... labels) {
        return new EdgeLabelFilter<>(labels);
    }

    @Override
    public boolean filter(IEdge<K, EV> value) {
        return labels.contains(((IGraphElementWithLabelField) value).getLabel());
    }

    public Set<String> getLabels() {
        return labels;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.EDGE_LABEL;
    }

    @Override
    public String toString() {
        return String.format("\"%s(%s)\"", getFilterType().name(),
            Joiner.on(',').join(labels));
    }
}
