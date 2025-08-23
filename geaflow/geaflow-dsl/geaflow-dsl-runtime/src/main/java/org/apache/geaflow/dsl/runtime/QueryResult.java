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

import java.io.Serializable;
import java.util.List;
import org.apache.geaflow.dsl.common.data.Row;

public class QueryResult implements Serializable {

    private List<Row> results;

    private RDataView dataView;

    private boolean success;

    private String errorMsg;

    public QueryResult(List<Row> results, RDataView dataView, boolean success, String errorMsg) {
        this.results = results;
        this.dataView = dataView;
        this.success = success;
        this.errorMsg = errorMsg;
    }

    public QueryResult(List<Row> results) {
        this(results, null, true, null);
    }

    public QueryResult(boolean success) {
        this(null, null, success, null);
    }

    public QueryResult(RDataView dataView) {
        this(null, dataView, true, null);
    }

    public QueryResult() {
    }

    public List<Row> getResults() {
        return results;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setResults(List<Row> results) {
        this.results = results;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public RDataView getDataView() {
        return dataView;
    }

    public void setDataView(RDataView dataView) {
        this.dataView = dataView;
    }
}
