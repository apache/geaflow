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

package org.apache.geaflow.ai;

import org.apache.geaflow.ai.operator.SessionOperator;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.session.SessionManagement;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.subgraph.reduce.ReduceFunction;
import org.apache.geaflow.ai.verbalization.Context;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;

import java.util.List;

public class GraphMemoryServer {

    private final SessionManagement sessionManagement = SessionManagement.INSTANCE;

    public String createSession() {
        String sessionId = sessionManagement.createSession();
        if (sessionId == null) {
            throw new RuntimeException("Cannot create new session");
        }
        return sessionId;
    }

    public String vectorSearch(VectorSearch search) {
        String sessionId = search.getSessionId();
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Session id is empty");
        }
        if (!sessionManagement.sessionExists(sessionId)) {
            sessionManagement.createSession(sessionId);
        }
        SessionOperator op = new SessionOperator();
        //预处理search
        search.preProcess();
        if (op.vectorSearch(search)) {
            return search.getSessionId();
        }
        return null;
    }

    public String reduce(String sessionId, ReduceFunction reduceFunction) {
        return sessionId;
    }

    public Context verbalize(String sessionId, VerbalizationFunction verbalizationFunction) {
        List<SubGraph> subGraphList = sessionManagement.getSubGraph(sessionId);
        StringBuilder stringBuilder = new StringBuilder();
        for (SubGraph subGraph : subGraphList) {
            stringBuilder.append(subGraph.toString()).append("\n");
        }
        return new Context(stringBuilder.toString());
    }

}
