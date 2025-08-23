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

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.pushdown.StatePushDown;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.project.IProjector;
import org.apache.geaflow.state.strategy.manager.IGraphManager;

public class QueryableOneKeyGraphStateImpl<K, VV, EV, R>
    extends QueryableKeysGraphStateImpl<K, VV, EV, R> {

    public QueryableOneKeyGraphStateImpl(QueryType<R> type, IGraphManager<K, VV, EV> graphManager,
                                         QueryCondition<K> queryCondition) {
        super(type, graphManager, queryCondition);
    }

    @Override
    public QueryableGraphState<K, VV, EV, R> by(IFilter[] filters) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public QueryableGraphState<K, VV, EV, R> by(IFilter filter) {
        this.queryCondition.stateFilters[0] = filter;
        return this;
    }

    @Override
    public <U> QueryableGraphState<K, VV, EV, U> select(IProjector<R, U> projector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<R> iterator() {
        return IteratorWithClose.wrap(asList().iterator());
    }

    protected StatePushDown getPushDown() {
        return StatePushDown.of()
            .withFilter(filterConverter.convert(queryCondition.stateFilters[0]))
            .withEdgeLimit(queryCondition.limit)
            .withOrderField(queryCondition.order);
    }

    @Override
    public List<R> asList() {
        if (DataType.E == this.type.getType()) {
            return (List<R>) this.graphManager.getStaticGraphTrait().getEdges(
                queryCondition.queryId, getPushDown());
        } else {
            return Collections.singletonList(get());
        }
    }

    @Override
    public R get() {
        switch (this.type.getType()) {
            case V:
                return (R) this.graphManager.getStaticGraphTrait().getVertex(
                    queryCondition.queryId, getPushDown());
            case VE:
                return (R) this.graphManager.getStaticGraphTrait().getOneDegreeGraph(
                    queryCondition.queryId, getPushDown());
            default:
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.runError("not supported " + this.type.getType()));
        }

    }
}
