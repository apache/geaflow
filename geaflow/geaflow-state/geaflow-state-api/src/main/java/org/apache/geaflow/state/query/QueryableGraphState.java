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

import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.state.graph.encoder.EdgeAtom;
import org.apache.geaflow.state.pushdown.project.IProjector;

/**
 * The base query interface for graph.
 */
public interface QueryableGraphState<K, VV, EV, R> {

    /**
     * Query by some projector.
     */
    <U> QueryableGraphState<K, VV, EV, U> select(IProjector<R, U> projector);

    /**
     * Query by edge limit.
     */
    QueryableGraphState<K, VV, EV, R> limit(long out, long in);

    /**
     * Query by some order.
     */
    QueryableGraphState<K, VV, EV, R> orderBy(EdgeAtom atom);

    /**
     * Query a aggregate result.
     */
    Map<K, Long> aggregate();

    /**
     * Query result is a list.
     */
    List<R> asList();

    /**
     * Get id Iterator.
     */
    CloseableIterator<K> idIterator();

    /**
     * Query result is a iterator.
     */
    CloseableIterator<R> iterator();

    /**
     * Get a simple result like a vertex.
     */
    R get();
}
