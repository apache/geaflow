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

import java.util.Collection;
import java.util.List;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.query.QueryType;
import org.apache.geaflow.state.strategy.manager.IGraphManager;

public class DynamicVertexStateImpl<K, VV, EV> extends
    BaseDynamicQueryState<K, VV, EV, IVertex<K, VV>> implements
    DynamicVertexState<K, VV, EV> {

    public DynamicVertexStateImpl(IGraphManager<K, VV, EV> graphManager) {
        super(new QueryType<>(DataType.V), graphManager);
    }

    @Override
    public List<Long> getAllVersions(K id) {
        return this.graphManager.getDynamicGraphTrait().getAllVersions(id, DataType.V);
    }

    @Override
    public long getLatestVersion(K id) {
        return this.graphManager.getDynamicGraphTrait().getLatestVersion(id, DataType.V);
    }

    @Override
    public void add(long version, IVertex<K, VV> vertex) {
        this.graphManager.getDynamicGraphTrait().addVertex(version, vertex);
    }

    @Override
    public void update(long version, IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(long version, IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(long version, K... ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(long version, Collection<K> ids) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
