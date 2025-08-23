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
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.query.QueryType;
import org.apache.geaflow.state.strategy.manager.IGraphManager;

public class StaticEdgeStateImpl<K, VV, EV> extends BaseQueryState<K, VV, EV, IEdge<K, EV>> implements
    StaticEdgeState<K, VV, EV> {

    public StaticEdgeStateImpl(IGraphManager<K, VV, EV> manager) {
        super(new QueryType<>(DataType.E), manager);
    }

    @Override
    public void add(IEdge<K, EV> edge) {
        this.graphManager.getStaticGraphTrait().addEdge(edge);
    }

    @Override
    public void update(IEdge<K, EV> edge) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(IEdge<K, EV> edge) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(K... ids) {
        delete(Arrays.asList(ids));
    }

    @Override
    public void delete(Collection<K> ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public CloseableIterator<K> idIterator() {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
