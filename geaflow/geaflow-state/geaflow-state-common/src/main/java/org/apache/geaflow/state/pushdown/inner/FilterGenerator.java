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

package org.apache.geaflow.state.pushdown.inner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.data.TimeRange;
import org.apache.geaflow.state.pushdown.filter.AndFilter;
import org.apache.geaflow.state.pushdown.filter.EdgeLabelFilter;
import org.apache.geaflow.state.pushdown.filter.EdgeTsFilter;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.OrFilter;
import org.apache.geaflow.state.pushdown.filter.VertexLabelFilter;
import org.apache.geaflow.state.pushdown.filter.VertexTsFilter;
import org.apache.geaflow.state.pushdown.inner.PushDownPb.FilterNode;
import org.apache.geaflow.state.pushdown.inner.PushDownPb.IntList;
import org.apache.geaflow.state.pushdown.inner.PushDownPb.LongList;
import org.apache.geaflow.state.pushdown.inner.PushDownPb.StringList;

public class FilterGenerator {

    public static FilterPlanWithData getFilterPlanWithData(IFilter filter) {
        FilterNode plan = getFilterPlan(filter);
        FilterNode data = getFilterData(filter);
        return new FilterPlanWithData(plan, data);
    }

    private static FilterNode getFilterPlan(IFilter filter) {
        FilterType filterType = filter.getFilterType();
        switch (filterType) {
            case AND:
                return getLogicalPlan(filterType, ((AndFilter) filter).getFilters());
            case OR:
                return getLogicalPlan(filterType, ((OrFilter) filter).getFilters());
            case VERTEX_LABEL:
            case EDGE_LABEL:
            case VERTEX_TS:
            case EDGE_TS:
            case IN_EDGE:
            case OUT_EDGE:
            case VERTEX_VALUE_DROP:
            case EDGE_VALUE_DROP:
            case VERTEX_MUST_CONTAIN:
                return getNormalPlan(filterType);
            default:
                throw new GeaflowRuntimeException("not support user defined filter " + filter.getFilterType());
        }
    }

    private static FilterNode getLogicalPlan(FilterType filterType, List<IFilter> filters) {
        return FilterNode.newBuilder()
            .setFilterType(filterType.toPbFilterType())
            .addAllFilters(filters.stream()
                .sorted(Comparator.comparingInt(o -> o.getFilterType().ordinal()))
                .map(FilterGenerator::getFilterPlan).collect(Collectors.toList()))
            .build();
    }

    private static FilterNode getNormalPlan(FilterType filterType) {
        return FilterNode.newBuilder()
            .setFilterType(filterType.toPbFilterType())
            .build();
    }

    public static FilterNode getFilterData(IFilter filter) {
        FilterType filterType = filter.getFilterType();
        switch (filterType) {
            case AND:
                return getLogicalFilterData(filterType, ((AndFilter) filter).getFilters());
            case OR:
                return getLogicalFilterData(filterType, ((OrFilter) filter).getFilters());
            case VERTEX_LABEL:
                return getStringFilterData(filterType, ((VertexLabelFilter) filter).getLabels());
            case EDGE_LABEL:
                return getStringFilterData(filterType, ((EdgeLabelFilter) filter).getLabels());
            case VERTEX_TS:
                TimeRange range = ((VertexTsFilter) filter).getTimeRange();
                Collection<Long> longs = Arrays.asList(range.getStart(), range.getEnd());
                return getLongFilterData(filterType, longs);
            case EDGE_TS:
                range = ((EdgeTsFilter) filter).getTimeRange();
                longs = Arrays.asList(range.getStart(), range.getEnd());
                return getLongFilterData(filterType, longs);
            case IN_EDGE:
            case OUT_EDGE:
            case VERTEX_VALUE_DROP:
            case EDGE_VALUE_DROP:
            case VERTEX_MUST_CONTAIN:
            case EMPTY:
                return FilterNode.newBuilder()
                    .setFilterType(filterType.toPbFilterType())
                    .build();
            default:
                throw new GeaflowRuntimeException("not support user defined filter " + filter.getFilterType());
        }
    }

    private static FilterNode getLogicalFilterData(FilterType filterType, List<IFilter> filters) {
        return FilterNode.newBuilder()
            .setFilterType(filterType.toPbFilterType())
            .addAllFilters(filters.stream()
                .sorted(Comparator.comparingInt(o -> o.getFilterType().ordinal()))
                .map(FilterGenerator::getFilterData).collect(Collectors.toList()))
            .build();
    }

    private static FilterNode getStringFilterData(FilterType filterType, Collection<String> strs) {
        return FilterNode.newBuilder()
            .setFilterType(filterType.toPbFilterType())
            .setStrContent(StringList.newBuilder().addAllStr(strs).build())
            .build();
    }

    private static FilterNode getLongFilterData(FilterType filterType, Collection<Long> longs) {
        return FilterNode.newBuilder()
            .setFilterType(filterType.toPbFilterType())
            .setLongContent(LongList.newBuilder().addAllLong(longs).build())
            .build();
    }

    private static FilterNode getIntFilterData(FilterType filterType, Collection<Integer> ints) {
        return FilterNode.newBuilder()
            .setFilterType(filterType.toPbFilterType())
            .setIntContent(IntList.newBuilder().addAllInt(ints).build())
            .build();
    }
}
