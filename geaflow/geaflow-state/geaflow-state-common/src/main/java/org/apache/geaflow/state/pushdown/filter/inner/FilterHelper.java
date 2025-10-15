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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.state.data.TimeRange;
import org.apache.geaflow.state.pushdown.filter.EdgeLabelFilter;
import org.apache.geaflow.state.pushdown.filter.EdgeTsFilter;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.OrFilter;
import org.apache.geaflow.state.pushdown.filter.VertexLabelFilter;
import org.apache.geaflow.state.pushdown.filter.VertexTsFilter;

public class FilterHelper {

    public static boolean isSingleLimit(IFilter[] filters) {
        int orNumber = 0;
        int singleOrNumber = 0;
        for (IFilter filter : filters) {
            if (filter.getFilterType() == FilterType.OR) {
                orNumber++;
                if (((OrFilter) filter).isSingleLimit()) {
                    singleOrNumber++;
                }
            }
        }
        Preconditions.checkArgument(singleOrNumber == 0 || orNumber == singleOrNumber,
            "some or filter is not single");
        return singleOrNumber > 0;
    }

    /**
     * Parse label from graph filter, only direct filter converter support.
     *
     * @param filter graph filter.
     * @return list of labels to be filtered.
     */
    public static List<String> parseLabel(IGraphFilter filter, boolean isVertex) {
        if (filter.getFilterType() == FilterType.OR) {
            return parseLabelOr(filter, isVertex);
        } else if (filter.getFilterType() == FilterType.AND) {
            return parseLabelAnd(filter, isVertex);
        } else {
            return parseLabelNormal(filter, isVertex);
        }
    }

    private static List<String> parseLabelOr(IGraphFilter filter, boolean isVertex) {
        List<String> labels = new ArrayList<>();
        OrGraphFilter orFilter = (OrGraphFilter) filter;

        if (orFilter.getFilterList().isEmpty()) {
            return labels;
        }

        for (IGraphFilter childFilter : orFilter.getFilterList()) {
            List<String> tmpLabels = parseLabel(childFilter, isVertex);
            if (tmpLabels.isEmpty()) {
                labels.clear();
                return labels;
            } else {
                labels.addAll(tmpLabels);
            }
        }

        return labels;
    }

    private static List<String> parseLabelAnd(IGraphFilter filter, boolean isVertex) {
        List<String> labels = new ArrayList<>();
        AndGraphFilter andFilter = (AndGraphFilter) filter;

        if (andFilter.getFilterList().isEmpty()) {
            return labels;
        }

        for (IGraphFilter childFilter : andFilter.getFilterList()) {
            labels.addAll(parseLabel(childFilter, isVertex));
            if (labels.size() > 1) {
                return new ArrayList<>();
            }
        }

        return labels;
    }

    private static List<String> parseLabelNormal(IGraphFilter filter, boolean isVertex) {
        List<String> labels = new ArrayList<>();
        if (!isVertex && filter.getFilterType() == FilterType.EDGE_LABEL) {
            EdgeLabelFilter labelFilter = (EdgeLabelFilter) filter.getFilter();
            labels.addAll(labelFilter.getLabels());
        } else if (isVertex && filter.getFilterType() == FilterType.VERTEX_LABEL) {
            VertexLabelFilter labelFilter = (VertexLabelFilter) filter.getFilter();
            labels.addAll(labelFilter.getLabels());
        }

        return labels;
    }

    /**
     * Parse dt from graph filter, only direct filter converter support.
     *
     * @param filter graph filter.
     * @return time range
     */
    public static TimeRange parseDt(IGraphFilter filter, boolean isVertex) {
        if (filter.getFilterType() == FilterType.OR || filter.getFilterType() == FilterType.AND) {
            return parseDtCompose(filter, isVertex);
        } else {
            return parseDtNormal(filter, isVertex);
        }
    }

    private static TimeRange parseDtCompose(IGraphFilter filter, boolean isVertex) {
        BaseComposeGraphFilter composeFilter = (BaseComposeGraphFilter) filter;
        if (composeFilter.getFilterList().isEmpty()) {
            return null;
        }

        TimeRange range = null;
        for (IGraphFilter childFilter : composeFilter.getFilterList()) {
            range = parseDtNormal(childFilter, isVertex);
            if (range != null) {
                return range;
            }
        }

        return null;
    }

    private static TimeRange parseDtNormal(IGraphFilter filter, boolean isVertex) {
        if (isVertex) {
            if (filter.contains(FilterType.VERTEX_TS)) {
                VertexTsFilter tsFilter = (VertexTsFilter) (filter.retrieve(FilterType.VERTEX_TS)
                    .getFilter());

                return tsFilter.getTimeRange();
            }
        } else {
            if (filter.contains(FilterType.EDGE_TS)) {
                EdgeTsFilter tsFilter = (EdgeTsFilter) (filter.retrieve(FilterType.EDGE_TS)
                    .getFilter());

                return tsFilter.getTimeRange();
            }
        }

        return null;
    }
}
