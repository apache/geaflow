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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api.model;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.ai.retrieval.model.document.SourceRef;
import org.apache.geaflow.ai.retrieval.model.evidence.Evidence;
import org.apache.geaflow.ai.retrieval.model.graph.GraphPathRef;

/** Wire success response DTO. */
public class RetrievalResponse {

    private String requestId;
    private String graphName;
    private String graphVersion;
    private List<Evidence> evidence = new ArrayList<>();
    private List<GraphPathRef> paths = new ArrayList<>();
    private List<SourceRef> sources = new ArrayList<>();
    private RetrievalTrace trace;
    private RetrievalBudget effectiveBudget;
    private List<String> degradedChannels = new ArrayList<>();

    public RetrievalResponse() {
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getGraphName() {
        return graphName;
    }

    public void setGraphName(String graphName) {
        this.graphName = graphName;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public void setGraphVersion(String graphVersion) {
        this.graphVersion = graphVersion;
    }

    public List<Evidence> getEvidence() {
        return evidence;
    }

    public void setEvidence(List<Evidence> evidence) {
        this.evidence = evidence == null ? new ArrayList<Evidence>() : new ArrayList<>(evidence);
    }

    public List<GraphPathRef> getPaths() {
        return paths;
    }

    public void setPaths(List<GraphPathRef> paths) {
        this.paths = paths == null ? new ArrayList<GraphPathRef>() : new ArrayList<>(paths);
    }

    public List<SourceRef> getSources() {
        return sources;
    }

    public void setSources(List<SourceRef> sources) {
        this.sources = sources == null ? new ArrayList<SourceRef>() : new ArrayList<>(sources);
    }

    public RetrievalTrace getTrace() {
        return trace;
    }

    public void setTrace(RetrievalTrace trace) {
        this.trace = trace;
    }

    public RetrievalBudget getEffectiveBudget() {
        return effectiveBudget;
    }

    public void setEffectiveBudget(RetrievalBudget effectiveBudget) {
        this.effectiveBudget = effectiveBudget;
    }

    public List<String> getDegradedChannels() {
        return degradedChannels;
    }

    public void setDegradedChannels(List<String> degradedChannels) {
        this.degradedChannels = degradedChannels == null
            ? new ArrayList<String>() : new ArrayList<>(degradedChannels);
    }
}
