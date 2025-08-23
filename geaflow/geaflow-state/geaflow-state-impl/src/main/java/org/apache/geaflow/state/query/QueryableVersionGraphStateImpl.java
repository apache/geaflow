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

package org.apache.geaflow.state.query;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.pushdown.StatePushDown;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.inner.IFilterConverter;
import org.apache.geaflow.state.strategy.manager.IGraphManager;

public class QueryableVersionGraphStateImpl<K, VV, EV, R> implements QueryableVersionGraphState<K, VV, EV, R> {

    protected final QueryType<R> type;
    protected final IGraphManager<K, VV, EV> graphManager;
    protected QueryCondition<K> queryCondition;
    protected IFilterConverter filterConverter;

    public QueryableVersionGraphStateImpl(QueryType<R> type,
                                          IGraphManager<K, VV, EV> graphManager,
                                          QueryCondition<K> queryCondition) {
        this.type = type;
        this.graphManager = graphManager;
        this.queryCondition = queryCondition;
        this.filterConverter = this.graphManager.getFilterConverter();
    }

    @Override
    public QueryableVersionGraphState<K, VV, EV, R> by(IFilter filter) {
        this.queryCondition.stateFilters[0] = filter;
        return this;
    }

    @Override
    public Map<Long, R> asMap() {
        Preconditions.checkArgument(queryCondition.queryIds.size() == 1);
        K id = queryCondition.queryIds.iterator().next();
        Map<Long, IVertex<K, VV>> res;
        if (this.type.getType() == DataType.V) {
            if (queryCondition.versions != null) {
                res = this.graphManager.getDynamicGraphTrait().getVersionData(id,
                    queryCondition.versions,
                    StatePushDown.of().withFilter(filterConverter.convert(queryCondition.stateFilters[0])),
                    DataType.V);
            } else {
                res = this.graphManager.getDynamicGraphTrait().getAllVersionData(id,
                    StatePushDown.of().withFilter(filterConverter.convert(queryCondition.stateFilters[0])),
                    DataType.V);
            }
        } else {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
        }
        return (Map<Long, R>) res;
    }
}
