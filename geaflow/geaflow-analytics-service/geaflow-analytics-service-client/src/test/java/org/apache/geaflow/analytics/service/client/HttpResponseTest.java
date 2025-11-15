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

package org.apache.geaflow.analytics.service.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HttpResponseTest {

    @Test
    public void testSuccessfulResponse() throws IOException {
        QueryError queryError = new QueryError("success", 0);
        QueryResults queryResults = new QueryResults("query-1", queryError);
        byte[] serializedData = SerializerFactory.getKryoSerializer().serialize(queryResults);

        CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(mockClient.execute(Mockito.any(HttpPost.class))).thenReturn(mockResponse);
        Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        Mockito.when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        Mockito.when(mockStatusLine.toString()).thenReturn("HTTP/1.1 200 OK");
        Mockito.when(mockResponse.getAllHeaders()).thenReturn(new org.apache.http.Header[0]);
        Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(serializedData));

        HttpPost request = new HttpPost("http://localhost:8080/test");
        HttpResponse httpResponse = HttpResponse.execute(mockClient, request);

        Assert.assertTrue(httpResponse.enableQuerySuccess());
        Assert.assertEquals(httpResponse.getStatusCode(), HttpStatus.SC_OK);
        Assert.assertNotNull(httpResponse.getValue());
        Assert.assertEquals(httpResponse.getValue().getQueryId(), "query-1");
    }

    @Test
    public void testFailedResponse() throws IOException {
        byte[] errorData = "Internal Server Error".getBytes();

        CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(mockClient.execute(Mockito.any(HttpPost.class))).thenReturn(mockResponse);
        Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        Mockito.when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        Mockito.when(mockStatusLine.toString()).thenReturn("HTTP/1.1 500 Internal Server Error");
        Mockito.when(mockResponse.getAllHeaders()).thenReturn(new org.apache.http.Header[0]);
        Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(errorData));

        HttpPost request = new HttpPost("http://localhost:8080/test");
        HttpResponse httpResponse = HttpResponse.execute(mockClient, request);

        Assert.assertFalse(httpResponse.enableQuerySuccess());
        Assert.assertEquals(httpResponse.getStatusCode(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
        Assert.assertNotNull(httpResponse.getException());
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testGetValueOnFailedResponse() throws IOException {
        byte[] errorData = "Error".getBytes();

        CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(mockClient.execute(Mockito.any(HttpPost.class))).thenReturn(mockResponse);
        Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        Mockito.when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);
        Mockito.when(mockStatusLine.toString()).thenReturn("HTTP/1.1 400 Bad Request");
        Mockito.when(mockResponse.getAllHeaders()).thenReturn(new org.apache.http.Header[0]);
        Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(errorData));

        HttpPost request = new HttpPost("http://localhost:8080/test");
        HttpResponse httpResponse = HttpResponse.execute(mockClient, request);

        httpResponse.getValue(); // Should throw exception
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testExecuteWithIOException() throws IOException {
        CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
        Mockito.when(mockClient.execute(Mockito.any(HttpPost.class)))
            .thenThrow(new IOException("Connection refused"));

        HttpPost request = new HttpPost("http://localhost:8080/test");
        HttpResponse.execute(mockClient, request);
    }

    @Test
    public void testGetHeaders() throws IOException {
        QueryError queryError = new QueryError("success", 0);
        QueryResults queryResults = new QueryResults("query-1", queryError);
        byte[] serializedData = SerializerFactory.getKryoSerializer().serialize(queryResults);

        CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);

        org.apache.http.Header[] headers = new org.apache.http.Header[0];

        Mockito.when(mockClient.execute(Mockito.any(HttpPost.class))).thenReturn(mockResponse);
        Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        Mockito.when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        Mockito.when(mockStatusLine.toString()).thenReturn("HTTP/1.1 200 OK");
        Mockito.when(mockResponse.getAllHeaders()).thenReturn(headers);
        Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(serializedData));

        HttpPost request = new HttpPost("http://localhost:8080/test");
        HttpResponse httpResponse = HttpResponse.execute(mockClient, request);

        Assert.assertNotNull(httpResponse.getHeaders());
        Assert.assertEquals(httpResponse.getHeaders().length, 0);
    }

    @Test
    public void testToString() throws IOException {
        QueryError queryError = new QueryError("success", 0);
        QueryResults queryResults = new QueryResults("query-1", queryError);
        byte[] serializedData = SerializerFactory.getKryoSerializer().serialize(queryResults);

        CloseableHttpClient mockClient = Mockito.mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = Mockito.mock(StatusLine.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(mockClient.execute(Mockito.any(HttpPost.class))).thenReturn(mockResponse);
        Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        Mockito.when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        Mockito.when(mockStatusLine.toString()).thenReturn("HTTP/1.1 200 OK");
        Mockito.when(mockResponse.getAllHeaders()).thenReturn(new org.apache.http.Header[0]);
        Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(serializedData));

        HttpPost request = new HttpPost("http://localhost:8080/test");
        HttpResponse httpResponse = HttpResponse.execute(mockClient, request);

        String toString = httpResponse.toString();
        Assert.assertNotNull(toString);
        Assert.assertTrue(toString.contains("statusCode"));
        Assert.assertTrue(toString.contains("querySuccess"));
    }
}
