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

package org.apache.geaflow.dsl.runtime;

import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.rel.GraphAlgorithm;
import org.apache.geaflow.dsl.rel.GraphMatch;

/**
 * The runtime graph view which mapping logical graph operator to the runtime
 * representation of the underlying engine.
 */
public interface RuntimeGraph extends RDataView {

    List<Path> take(IType<?> type);

    RuntimeGraph traversal(GraphMatch graphMatch);

    RuntimeTable getPathTable();

    RuntimeTable runAlgorithm(GraphAlgorithm graphAlgorithm);

    default ViewType getType() {
        return ViewType.GRAPH;
    }
}
