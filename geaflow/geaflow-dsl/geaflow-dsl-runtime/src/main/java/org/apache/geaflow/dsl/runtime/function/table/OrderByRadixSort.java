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

import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.function.FunctionContext;
import org.apache.geaflow.dsl.runtime.function.table.order.SortInfo;
import org.apache.geaflow.dsl.runtime.function.table.order.MultiFieldRadixSort;
import java.util.ArrayList;
import java.util.List;

public class OrderByRadixSort implements OrderByFunction {

    private final SortInfo sortInfo;

    private List<Row> allRows;

    public OrderByRadixSort(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }

    @Override
    public void open(FunctionContext context) {
        this.allRows = new ArrayList<>();
    }

    @Override
    public void process(Row row) {
        if (sortInfo.fetch == 0) {
            return;
        }
        allRows.add(row);
    }

    @Override
    public Iterable<Row> finish() {
        List<Row> sortedRows = new ArrayList<>(allRows);
        MultiFieldRadixSort.multiFieldRadixSort(sortedRows, sortInfo);
        allRows.clear();
        return sortedRows;
    }
}
