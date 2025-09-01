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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.geaflow.cluster.response.ResponseResult;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryResultTest {

    private static final String ERROR_MSG = "server execute failed";

    @Test
    public void testQueryError() {
        QueryError queryError = new QueryError(ERROR_MSG);
        Assert.assertEquals(queryError.getCode(), 0);
        Assert.assertEquals(queryError.getName(), ERROR_MSG);
        queryError = new QueryError(ERROR_MSG, 1);
        Assert.assertEquals(queryError.getCode(), 1);
        Assert.assertEquals(queryError.getName(), ERROR_MSG);
    }

    @Test
    public void testQueryResult() {
        String queryId = "1";
        QueryResults queryResults = new QueryResults(queryId, new QueryError(ERROR_MSG));
        Assert.assertNull(queryResults.getFormattedData());
        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertEquals(queryResults.getError().getName(), ERROR_MSG);
        queryResults = new QueryResults(queryId, new QueryError(ERROR_MSG));
        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertEquals(queryResults.getError().getName(), ERROR_MSG);
    }

    @Test
    public void testQueryStatus() {
        String queryId = "1";
        List<List<ResponseResult>> result = new ArrayList<>();
        ArrayList<ResponseResult> responseResults = new ArrayList<>();
        responseResults.add(new ResponseResult(1, OutputType.RESPONSE,
            Collections.singletonList("result")));
        result.add(responseResults);
        QueryResults queryResults = new QueryResults(queryId, new QueryError(ERROR_MSG));
        Assert.assertNull(queryResults.getFormattedData());
        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertEquals(queryResults.getError().getName(), ERROR_MSG);
        Assert.assertFalse(queryResults.getQueryStatus());

        queryResults = new QueryResults(queryId, result);

        Assert.assertEquals(queryResults.getQueryId(), queryId);
        Assert.assertTrue(queryResults.getQueryStatus());
    }

    @Test
    public void testQueryResultWriteAndReadObjectWithData() throws IOException, ClassNotFoundException {
        String queryId = "1";
        List<List<ResponseResult>> result = new ArrayList<>();
        ArrayList<ResponseResult> responseResults = new ArrayList<>();
        responseResults.add(new ResponseResult(1, OutputType.RESPONSE,
            Collections.singletonList("result")));
        result.add(responseResults);
        QueryResults queryResults = new QueryResults(queryId, result);
        queryResults.setResultMeta(new RelRecordType(new ArrayList<>()));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(queryResults);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        QueryResults deserializedQueryResults = (QueryResults) objectInputStream.readObject();
        Assert.assertEquals(deserializedQueryResults.getQueryId(), queryId);
        Assert.assertTrue(deserializedQueryResults.getQueryStatus());
        Assert.assertTrue(deserializedQueryResults.getResultMeta().getFieldNames().isEmpty());
        outputStream.close();
        inputStream.close();
    }

    @Test
    public void testQueryResultWriteAndReadObjectWithQueryError() throws IOException,
        ClassNotFoundException {
        String queryId = "1";
        QueryResults queryResults = new QueryResults(queryId, new QueryError(ERROR_MSG));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(queryResults);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        QueryResults deserializedQueryResults = (QueryResults) objectInputStream.readObject();
        Assert.assertEquals(deserializedQueryResults.getQueryId(), queryId);
        Assert.assertFalse(deserializedQueryResults.getQueryStatus());
        Assert.assertEquals(deserializedQueryResults.getError().getName(), ERROR_MSG);
        outputStream.close();
        inputStream.close();
    }

    @Test
    public void testQueryResultWriteAndReadObjectWithKryo() {
        String queryId = "1";
        List<List<ResponseResult>> result = new ArrayList<>();
        ArrayList<ResponseResult> responseResults = new ArrayList<>();
        responseResults.add(new ResponseResult(1, OutputType.RESPONSE,
            Collections.singletonList("result")));
        result.add(responseResults);
        QueryResults queryResults = new QueryResults(queryId, result);
        queryResults.setResultMeta(new RelRecordType(new ArrayList<>()));
        byte[] serialize = SerializerFactory.getKryoSerializer().serialize(queryResults);
        QueryResults deserializedQueryResults = (QueryResults) SerializerFactory.getKryoSerializer().deserialize(serialize);
        Assert.assertEquals(deserializedQueryResults.getQueryId(), queryId);
        Assert.assertTrue(deserializedQueryResults.getQueryStatus());
        String expectResult = "{\"viewResult\":{\"nodes\":[],\"edges\":[]},"
            + "\"jsonResult\":[\"result\"]}";
        Assert.assertEquals(deserializedQueryResults.getFormattedData(), expectResult);
        Assert.assertTrue(deserializedQueryResults.getResultMeta().getFieldNames().isEmpty());
    }

}
