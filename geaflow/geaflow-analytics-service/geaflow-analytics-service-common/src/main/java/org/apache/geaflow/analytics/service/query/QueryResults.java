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

package org.apache.geaflow.analytics.service.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.geaflow.cluster.response.ResponseResult;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;

/* This file is based on source code from the Presto Project (https://prestodb.github.io/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Presto's com.facebook.presto.client.QueryResults.
 */
public class QueryResults implements IQueryStatus, Externalizable {
    private static final String DEFAULT_QUERY_ID = "0";
    private static final String SEPARATOR = ":";
    private String queryId;
    private List<List<ResponseResult>> queryOriginData;
    private List<List<Object>> rawData;
    private QueryError error;
    private boolean queryStatus;
    private RelDataType resultMeta;

    public QueryResults() {

    }

    public QueryResults(String queryId, List<List<ResponseResult>> data, QueryError error, boolean queryStatus) {
        this.queryId = queryId;
        this.queryOriginData = data;
        this.rawData = getRawData();
        this.error = error;
        this.queryStatus = queryStatus;
    }

    public RelDataType getResultMeta() {
        return resultMeta;
    }

    public void setResultMeta(RelDataType resultMeta) {
        this.resultMeta = resultMeta;
    }

    public QueryResults(String queryId, List<List<ResponseResult>> data) {
        this(queryId, data, null, true);
    }

    public QueryResults(String queryId, QueryError error) {
        this(queryId, null, error, false);
    }

    public QueryResults(QueryError error) {
        this(DEFAULT_QUERY_ID, null, error, false);
    }

    @Override
    public String getQueryId() {
        return queryId;
    }


    public String getFormattedData() {
        if (this.queryOriginData != null) {
            return DefaultResultSetFormatUtils.formatResult(this.queryOriginData, this.resultMeta);
        }
        return null;
    }

    public List<List<Object>> getRawData() {
        List<List<Object>> result = new ArrayList<>();
        if (queryOriginData == null) {
            return result;
        }
        for (List<ResponseResult> responseResults : queryOriginData) {
            for (ResponseResult responseResult : responseResults) {
                for (Object response : responseResult.getResponse()) {
                    if (response == null) {
                        continue;
                    }
                    if (response instanceof ObjectRow) {
                        ObjectRow objectRow = (ObjectRow) response;
                        Object[] fields = objectRow.getFields();
                        result.add(Arrays.asList(fields));
                    } else {
                        result.add(Collections.singletonList(response));
                    }
                }
            }
        }
        return result;
    }

    public QueryError getError() {
        return error;
    }

    public boolean getQueryStatus() {
        return queryStatus;
    }

    @Override
    public String toString() {
        return this.queryId + SEPARATOR + this.getQueryStatus() + SEPARATOR + (this.getQueryStatus() ? this.rawData : this.error.toString());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(queryId);
        out.writeBoolean(queryStatus);
        if (queryStatus) {
            out.writeInt(this.rawData.size());
            for (List<Object> rawDatum : rawData) {
                out.writeInt(rawDatum.size());
                for (Object data : rawDatum) {
                    out.writeObject(data);
                }
            }
            out.writeObject(resultMeta);
        } else {
            out.writeObject(error);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        queryId = (String) in.readObject();
        queryStatus = in.readBoolean();
        if (queryStatus) {
            int dataSize = in.readInt();
            List<List<Object>> rawData = new ArrayList<>(dataSize);
            for (int i = 0; i < dataSize; i++) {
                int datumSize = in.readInt();
                List<Object> datum = new ArrayList<>();
                for (int j = 0; j < datumSize; j++) {
                    datum.add(in.readObject());
                }
                rawData.add(datum);
            }
            this.rawData = rawData;
            resultMeta = (RelDataType) in.readObject();
        } else {
            error = (QueryError) in.readObject();
        }
    }
}
