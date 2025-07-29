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

package com.antgroup.geaflow.state.query;

import com.antgroup.geaflow.state.graph.encoder.EdgeAtom;
import com.antgroup.geaflow.state.pushdown.filter.EmptyFilter;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import com.antgroup.geaflow.state.pushdown.limit.IEdgeLimit;
import com.antgroup.geaflow.state.pushdown.project.IProjector;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import java.util.Collection;
import java.util.List;

public class QueryCondition<K> {

    public Collection<Long> versions;
    public K queryId;
    public List<K> queryIds;
    public boolean isFullScan;
    public IFilter[] stateFilters = new IFilter[] {EmptyFilter.getInstance()};
    public IProjector projector;
    public IEdgeLimit limit;
    public EdgeAtom order;
    public KeyGroup keyGroup;
}
