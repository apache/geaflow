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

package org.apache.geaflow.dsl.runtime.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.RDataView;

public interface PhysicRelNode<D extends RDataView> extends RelNode {

    D translate(QueryContext context);

    String showSQL();

    enum PhysicRelNodeName {
        /**
         * Name for table scan node.
         */
        TABLE_SCAN("TableScan"),
        /**
         * Name for project node.
         */
        PROJECT("Project"),
        /**
         * Name for filter node.
         */
        FILTER("Filter"),
        /**
         * Name for aggregate node.
         */
        AGGREGATE("Aggregate"),
        /**
         * Name for join node.
         */
        JOIN("Join"),
        /**
         * Name for table function node.
         */
        TABLE_FUNCTION("TableFunction"),
        /**
         * Name for union node.
         */
        UNION("Union"),
        /**
         * Name for sort node.
         */
        SORT("Sort"),
        /**
         * Name for correlate node.
         */
        CORRELATE("Correlate"),
        /**
         * Name for value scan node.
         */
        VALUE_SCAN("ValueScan"),
        /**
         * Name for graph scan node.
         */
        GRAPH_SCAN("GraphScan"),
        /**
         * Name for graph match node.
         */
        GRAPH_MATCH("GraphMatch"),
        /**
         * Name for table sink node.
         */
        TABLE_SINK("TableSink");

        private final String namePrefix;

        PhysicRelNodeName(String namePrefix) {
            this.namePrefix = namePrefix;
        }

        public String getName(Object suffix) {
            return namePrefix + "-" + suffix;
        }
    }
}
