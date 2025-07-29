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

package org.apache.geaflow.state;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.state.query.QueryCondition;
import org.apache.geaflow.state.query.QueryType;
import org.apache.geaflow.state.query.QueryableAllGraphState;
import org.apache.geaflow.state.query.QueryableAllGraphStateImpl;
import org.apache.geaflow.state.query.QueryableKeysGraphState;
import org.apache.geaflow.state.query.QueryableKeysGraphStateImpl;
import org.apache.geaflow.state.query.QueryableVersionGraphState;
import org.apache.geaflow.state.query.QueryableVersionGraphStateImpl;
import org.apache.geaflow.state.strategy.manager.IGraphManager;
import org.apache.geaflow.utils.keygroup.KeyGroup;

public abstract class BaseDynamicQueryState<K, VV, EV, R> implements
    DynamicQueryableState<K, VV, EV, R> {

    protected final QueryType<R> queryType;
    protected final IGraphManager<K, VV, EV> graphManager;

    public BaseDynamicQueryState(QueryType<R> queryType, IGraphManager<K, VV, EV> graphManager) {
        this.queryType = queryType;
        this.graphManager = graphManager;
    }

    @Override
    public QueryableAllGraphState<K, VV, EV, R> query(long version) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = null;
        queryCondition.isFullScan = true;
        return new QueryableAllGraphStateImpl<>(version, queryType, graphManager, queryCondition);
    }

    @Override
    public QueryableAllGraphState<K, VV, EV, R> query(long version, KeyGroup keyGroup) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.keyGroup = keyGroup;
        queryCondition.queryIds = null;
        queryCondition.isFullScan = true;
        return new QueryableAllGraphStateImpl<>(version, queryType, graphManager, queryCondition);
    }

    @Override
    public QueryableKeysGraphState<K, VV, EV, R> query(long version, K... ids) {
        return query(version, Arrays.asList(ids));
    }

    @Override
    public QueryableKeysGraphState<K, VV, EV, R> query(long version, List<K> ids) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = ids;
        queryCondition.isFullScan = false;
        return new QueryableKeysGraphStateImpl<>(version, queryType, graphManager, queryCondition);
    }

    @Override
    public QueryableVersionGraphState<K, VV, EV, R> query(K id) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = Arrays.asList(id);
        return new QueryableVersionGraphStateImpl<>(queryType, graphManager, queryCondition);
    }

    @Override
    public QueryableVersionGraphState<K, VV, EV, R> query(K id, Collection<Long> versions) {
        QueryCondition<K> queryCondition = new QueryCondition<>();
        queryCondition.queryIds = Arrays.asList(id);
        queryCondition.versions = versions;
        return new QueryableVersionGraphStateImpl<>(queryType, graphManager, queryCondition);
    }

    @Override
    public CloseableIterator<K> idIterator() {
        return this.graphManager.getDynamicGraphTrait().vertexIDIterator();
    }
}
