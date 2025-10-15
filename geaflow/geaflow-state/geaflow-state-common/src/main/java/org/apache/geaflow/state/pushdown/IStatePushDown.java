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

package org.apache.geaflow.state.pushdown;

import java.util.List;
import java.util.Map;
import org.apache.geaflow.state.graph.encoder.EdgeAtom;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.limit.IEdgeLimit;
import org.apache.geaflow.state.pushdown.project.IProjector;

public interface IStatePushDown<K, T, R> {

    IFilter getFilter();

    <F extends IFilter> Map<K, F> getFilters();

    IEdgeLimit getEdgeLimit();

    List<EdgeAtom> getOrderFields();

    IProjector<T, R> getProjector();

    PushDownType getType();

    boolean isEmpty();
}
