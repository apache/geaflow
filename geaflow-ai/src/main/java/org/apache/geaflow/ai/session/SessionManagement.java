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

package org.apache.geaflow.ai.session;

import java.util.*;
import org.apache.geaflow.ai.common.config.Constants;
import org.apache.geaflow.ai.subgraph.SubGraph;

public class SessionManagement {

    private final Map<String, Long> session2ActiveTime = new HashMap<>();
    private final Map<String, List<SubGraph>> session2Graphs = new HashMap<>();

    public SessionManagement() {
    }

    public boolean createSession(String sessionId) {
        if (session2ActiveTime.containsKey(sessionId)) {
            return false;
        }
        session2ActiveTime.put(sessionId, System.nanoTime());
        session2Graphs.putIfAbsent(sessionId, new ArrayList<>());
        return true;
    }

    public String createSession() {
        String sessionId = Constants.PREFIX_TMP_SESSION + System.nanoTime()
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        if (createSession(sessionId)) {
            return sessionId;
        } else {
            return null;
        }
    }

    public boolean sessionExists(String session) {
        return this.session2ActiveTime.containsKey(session);
    }

    public List<SubGraph> getSubGraph(String sessionId) {
        List<SubGraph> l = this.session2Graphs.get(sessionId);
        return l == null ? new ArrayList<>() : l;
    }

    public void setSubGraph(String sessionId, List<SubGraph> subGraphs) {
        this.session2Graphs.put(sessionId, subGraphs);
    }
}
