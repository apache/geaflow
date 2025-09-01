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

package org.apache.geaflow.dsl.runtime;

import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.function.table.AggFunction;
import org.apache.geaflow.dsl.runtime.function.table.CorrelateFunction;
import org.apache.geaflow.dsl.runtime.function.table.GroupByFunction;
import org.apache.geaflow.dsl.runtime.function.table.JoinTableFunction;
import org.apache.geaflow.dsl.runtime.function.table.OrderByFunction;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunction;
import org.apache.geaflow.dsl.runtime.function.table.WhereFunction;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowTable;

/**
 * The runtime table view which  mapping SQL function to the runtime
 * representation of the underlying engine.
 */
public interface RuntimeTable extends RDataView {

    RuntimeTable project(ProjectFunction function);

    RuntimeTable filter(WhereFunction function);

    RuntimeTable join(RuntimeTable other, JoinTableFunction function);

    RuntimeTable aggregate(GroupByFunction groupByFunction, AggFunction aggFunction);

    RuntimeTable union(RuntimeTable other);

    RuntimeTable orderBy(OrderByFunction function);

    RuntimeTable correlate(CorrelateFunction function);

    SinkDataView write(GeaFlowTable table);

    SinkDataView write(GeaFlowGraph graph, QueryContext queryContext);

    List<Row> take(IType<?> type);

    default ViewType getType() {
        return ViewType.TABLE;
    }
}
