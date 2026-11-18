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

package org.apache.geaflow.ai.retrieval.channel.graph;

import java.util.Collections;
import java.util.Map;

/** Statistics for one vertex in an immutable graph view. */
public final class GraphVertexStatistics {

    private final String entityId;
    private final String statisticsVersion;
    private final int degree;
    private final Map<String, Integer> relationCounts;

    GraphVertexStatistics(String entityId, String statisticsVersion, int degree,
                          Map<String, Integer> relationCounts) {
        this.entityId = entityId;
        this.statisticsVersion = statisticsVersion;
        this.degree = degree;
        this.relationCounts = relationCounts == null ? Collections.emptyMap() : relationCounts;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getStatisticsVersion() {
        return statisticsVersion;
    }

    public int getDegree() {
        return degree;
    }

    public int degree() {
        return degree;
    }

    public Map<String, Integer> getRelationCounts() {
        return relationCounts;
    }

    public Map<String, Integer> relationCounts() {
        return relationCounts;
    }
}
