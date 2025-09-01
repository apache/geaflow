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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Presto's com.facebook.presto.client.JsonResponse.
 */
public class HttpResponse {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponse.class);
    private static final int RESULT_BUFFER_SIZE = 4096;
    private static final String STATUS_CODE = "statusCode";
    private static final String STATUS_MESSAGE = "statusMessage";
    private static final String HEADERS = "headers";
    private static final String QUERY_SUCCESS_KEY = "querySuccess";
    private static final String VALUE_KEY = "value";

    private final int statusCode;

    private final String statusMessage;

    private final Header[] headers;
    private byte[] responseBytes;
    private boolean querySuccess;
    private QueryResults value;

    private GeaflowRuntimeException exception;

    private HttpResponse(int statusCode, String statusMessage, Header[] responseAllHeaders,
                         byte[] responseBytes) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(responseAllHeaders, "headers is null");
        this.responseBytes = requireNonNull(responseBytes, "responseBytes is null");
        if (statusCode == HttpStatus.SC_OK) {
            this.value = (QueryResults) SerializerFactory.getKryoSerializer().deserialize(responseBytes);
            this.querySuccess = true;
        } else {
            String response = new String(responseBytes, StandardCharsets.UTF_8);
            exception = new GeaflowRuntimeException(String.format("analytics http request failed,"
                + " status code: %s, status message: %s, response: %s", statusCode, statusMessage, response));
            this.querySuccess = false;
        }
    }

    public static HttpResponse execute(CloseableHttpClient client, HttpUriRequest request) {
        try (CloseableHttpResponse response = client.execute(request)) {
            HttpEntity entity = response.getEntity();
            InputStream content = entity.getContent();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[RESULT_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = content.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            byte[] resultBytes = outputStream.toByteArray();
            outputStream.close();
            content.close();
            int responseCode = response.getStatusLine().getStatusCode();
            Header[] responseAllHeaders = response.getAllHeaders();
            String responseMessage = response.getStatusLine().toString();
            return new HttpResponse(responseCode, responseMessage, responseAllHeaders, resultBytes);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(String.format("execute analytics http request %s "
                + "fail", request.getURI().toString()), e);
        }
    }


    public boolean enableQuerySuccess() {
        return querySuccess;
    }

    public QueryResults getValue() {
        if (!querySuccess) {
            throw new GeaflowRuntimeException("Response does not contain value", exception);
        }
        return value;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Header[] getHeaders() {
        return headers;
    }

    public GeaflowRuntimeException getException() {
        return exception;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add(STATUS_CODE, statusCode)
            .add(STATUS_MESSAGE, statusMessage)
            .add(HEADERS, Arrays.toString(headers))
            .add(QUERY_SUCCESS_KEY, querySuccess)
            .add(VALUE_KEY, value)
            .omitNullValues()
            .toString();
    }

}
