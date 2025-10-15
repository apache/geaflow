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

import org.apache.geaflow.state.pushdown.filter.IFilter;

/**
 * The point query interface for graph.
 */
public interface QueryableKeysGraphState<K, VV, EV, R> extends QueryableGraphState<K, VV, EV, R> {

    /**
     * Query by the filters, corresponding to the search keys.
     */
    QueryableGraphState<K, VV, EV, R> by(IFilter... filter);

    /**
     * Query by a filter, sharing by the search keys.
     */
    QueryableGraphState<K, VV, EV, R> by(IFilter filter);
}
