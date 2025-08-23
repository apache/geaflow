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

package org.apache.geaflow.dsl.runtime.function.table;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.function.FunctionContext;
import org.apache.geaflow.dsl.runtime.function.table.order.SortInfo;
import org.apache.geaflow.dsl.runtime.function.table.order.TopNRowComparator;

public class OrderByFunctionImpl implements OrderByFunction {

    private final SortInfo sortInfo;

    private PriorityQueue<Row> topNQueue;

    private List<Row> allRows;

    private TopNRowComparator<Row> topNRowComparator;

    public OrderByFunctionImpl(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }

    @Override
    public void open(FunctionContext context) {
        this.topNRowComparator = new TopNRowComparator<>(sortInfo);
        if (sortInfo.fetch > 0) {
            this.topNQueue = new PriorityQueue<>(
                sortInfo.fetch, topNRowComparator.getNegativeComparator());
        } else {
            this.allRows = new ArrayList<>();
        }
    }

    @Override
    public void process(Row row) {
        if (sortInfo.fetch == 0) {
            return;
        }
        if (topNQueue != null) {
            if (topNQueue.size() == sortInfo.fetch) {
                if (sortInfo.orderByFields.isEmpty()) {
                    return;
                }
                Row top = topNQueue.peek();
                if (topNQueue.comparator().compare(top, row) < 0) {
                    topNQueue.remove();
                    topNQueue.add(row);
                }
            } else {
                topNQueue.add(row);
            }
        } else {
            allRows.add(row);
        }
    }

    @Override
    public Iterable<Row> finish() {
        if (topNQueue != null) {
            List<Row> results = Lists.newArrayList(topNQueue.iterator());
            results.sort(topNRowComparator);
            topNQueue.clear();
            return results;
        } else {
            List<Row> sortedRows = new ArrayList<>(allRows);
            sortedRows.sort(topNRowComparator);
            allRows.clear();
            return sortedRows;
        }
    }
}
