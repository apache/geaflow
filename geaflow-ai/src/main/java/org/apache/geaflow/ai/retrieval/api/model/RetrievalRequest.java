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

import java.util.List;

/** Wire request DTO. Validation is performed by RetrievalRequestValidator. */
public class RetrievalRequest {

    private String graphName;
    private String requestId;
    private String query;
    private List<Double> queryVector;
    private String mode;
    private String executionMode;
    private RetrievalBudget budget;
    private String graphVersion;
    private String indexVersion;
    private String vectorVersion;
    private String vectorSource;
    private boolean allowPartialResults;

    public RetrievalRequest() {
    }

    public String getGraphName() {
        return graphName;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public boolean isAllowPartialResults() {
        return allowPartialResults;
    }

    public void setAllowPartialResults(boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
    }

    public void setGraphName(String graphName) {
        this.graphName = graphName;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<Double> getQueryVector() {
        return queryVector;
    }

    public void setQueryVector(List<Double> queryVector) {
        this.queryVector = queryVector;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(String executionMode) {
        this.executionMode = executionMode;
    }

    public RetrievalBudget getBudget() {
        return budget;
    }

    public void setBudget(RetrievalBudget budget) {
        this.budget = budget;
    }

    public String getGraphVersion() {
        return graphVersion;
    }

    public void setGraphVersion(String graphVersion) {
        this.graphVersion = graphVersion;
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public void setIndexVersion(String indexVersion) {
        this.indexVersion = indexVersion;
    }

    public String getVectorVersion() {
        return vectorVersion;
    }

    public void setVectorVersion(String vectorVersion) {
        this.vectorVersion = vectorVersion;
    }

    public String getVectorSource() {
        return vectorSource;
    }

    public void setVectorSource(String vectorSource) {
        this.vectorSource = vectorSource;
    }
}
