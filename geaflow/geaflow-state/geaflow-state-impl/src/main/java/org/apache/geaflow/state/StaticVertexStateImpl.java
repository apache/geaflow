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
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.data.DataType;
import org.apache.geaflow.state.query.QueryType;
import org.apache.geaflow.state.strategy.manager.IGraphManager;

public class StaticVertexStateImpl<K, VV, EV> extends BaseQueryState<K, VV, EV, IVertex<K, VV>> implements
    StaticVertexState<K, VV, EV> {

    public StaticVertexStateImpl(IGraphManager<K, VV, EV> graphManager) {
        super(new QueryType<>(DataType.V), graphManager);
    }

    @Override
    public void add(IVertex<K, VV> vertex) {
        this.graphManager.getStaticGraphTrait().addVertex(vertex);
    }

    @Override
    public void update(IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(IVertex<K, VV> vertex) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public void delete(K... ids) {
        delete(Arrays.asList(ids));
    }

    @Override
    public void delete(Collection<K> id) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }
}
