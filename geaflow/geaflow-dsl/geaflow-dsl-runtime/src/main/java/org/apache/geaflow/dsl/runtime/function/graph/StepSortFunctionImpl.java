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

package org.apache.geaflow.dsl.runtime.function.graph;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField;
import org.apache.geaflow.dsl.runtime.function.table.order.SortInfo;
import org.apache.geaflow.dsl.runtime.function.table.order.TopNRowComparator;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.TreePaths;

public class StepSortFunctionImpl implements StepSortFunction {

    private final SortInfo sortInfo;

    private PriorityQueue<Pair<RowVertex, Path>> topNQueue;

    private List<Pair<RowVertex, Path>> paths;

    private TopNRowComparator<Path> topNComparator;

    private final boolean isGlobalSortFunction;

    public StepSortFunctionImpl(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
        this.isGlobalSortFunction = false;
    }

    private StepSortFunctionImpl(SortInfo sortInfo, boolean isGlobalSortFunction) {
        this.sortInfo = sortInfo;
        this.isGlobalSortFunction = isGlobalSortFunction;
    }

    public StepSortFunctionImpl copy(boolean isGlobalSortFunction) {
        return new StepSortFunctionImpl(sortInfo, isGlobalSortFunction);
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {
        this.topNComparator = new TopNRowComparator<>(sortInfo);
        if (sortInfo.fetch > 0) {
            this.topNQueue = new PriorityQueue<>(sortInfo.fetch,
                new RowPairComparator(topNComparator.getNegativeComparator()));
        } else {
            this.paths = new ArrayList<>();
        }
    }

    @Override
    public List<Expression> getExpressions() {
        return sortInfo.orderByFields.stream()
            .map(field -> field.expression)
            .collect(Collectors.toList());
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert sortInfo.orderByFields.size() == expressions.size();

        List<OrderByField> newOrderByFields = new ArrayList<>(sortInfo.orderByFields.size());
        for (int i = 0; i < expressions.size(); i++) {
            OrderByField newOrderByField = sortInfo.orderByFields.get(i).copy(expressions.get(i));
            newOrderByFields.add(newOrderByField);
        }
        return new StepSortFunctionImpl(sortInfo.copy(newOrderByFields), isGlobalSortFunction);
    }

    @Override
    public void process(RowVertex currentVertex, Path path) {
        if (sortInfo.fetch == 0) {
            return;
        }
        Pair<RowVertex, Path> newPair = Pair.of(currentVertex, path);
        if (topNQueue != null) {
            if (topNQueue.size() == sortInfo.fetch) {
                if (sortInfo.orderByFields.isEmpty()) {
                    return;
                }
                Pair<RowVertex, Path> top = topNQueue.peek();
                if (topNQueue.comparator().compare(top, newPair) < 0) {
                    topNQueue.remove();
                    topNQueue.add(newPair);
                }
            } else {
                topNQueue.add(newPair);
            }
        } else {
            paths.add(newPair);
        }
    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {
        List<Pair<RowVertex, Path>> topNPaths;
        if (topNQueue != null) {
            topNPaths = Lists.newArrayList(topNQueue.iterator());
            topNQueue.clear();
        } else {
            topNPaths = Lists.newArrayList(paths);
            paths.clear();
        }
        topNPaths.sort(new RowPairComparator(topNComparator));

        Map<RowVertex, List<Path>> head2PathsMap = new HashMap<>();
        for (Pair<RowVertex, Path> pair : topNPaths) {
            head2PathsMap.computeIfAbsent(pair.getKey(), x -> new ArrayList<>());
            head2PathsMap.get(pair.getKey()).add(pair.getValue());
        }
        for (RowVertex currentVertex : head2PathsMap.keySet()) {
            for (Path path : head2PathsMap.get(currentVertex)) {
                if (isGlobalSortFunction) {
                    collector.collect(VertexRecord.of(currentVertex,
                        TreePaths.createTreePath(Collections.singletonList(path))));
                } else {
                    Row row = ObjectRow.create(currentVertex, path);
                    collector.collect(row);
                }
            }
        }
    }

    protected class RowPairComparator implements Comparator<Pair<RowVertex, Path>> {

        private final Comparator comparator;

        public RowPairComparator(Comparator comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Pair a, Pair b) {
            return comparator.compare(a.getValue(), b.getValue());
        }
    }
}
