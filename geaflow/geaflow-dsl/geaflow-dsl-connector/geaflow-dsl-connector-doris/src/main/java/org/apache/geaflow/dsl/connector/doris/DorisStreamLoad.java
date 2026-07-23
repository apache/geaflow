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

package org.apache.geaflow.dsl.connector.doris;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lightweight Doris Stream Load client. It issues an HTTP PUT to the Doris FE, which replies
 * with a 307 redirect to a BE, and the request body is re-sent to the BE that finally executes
 * the load. The buffered payload is loaded in a single request, giving a much higher throughput
 * than row-by-row JDBC INSERT.
 */
public class DorisStreamLoad implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisStreamLoad.class);

    private final List<String> loadUrls;
    private final String authHeader;
    private final String format;
    private final String columnSeparator;
    private final String lineDelimiter;
    private final String columns;
    private final int maxRetries;
    private final CloseableHttpClient httpClient;
    private final Gson gson = new Gson();

    public DorisStreamLoad(List<String> feNodes, String database, String table, String username,
                           String password, String format, String columnSeparator,
                           String lineDelimiter, List<String> columns, int connectTimeoutMs,
                           int readTimeoutMs, int maxRetries) {
        this.loadUrls = new ArrayList<>();
        if (feNodes != null) {
            for (String feNode : feNodes) {
                if (feNode != null && !feNode.trim().isEmpty()) {
                    this.loadUrls.add(String.format(DorisConstants.STREAM_LOAD_URL_PATTERN,
                        normalizeFeNode(feNode), database, table));
                }
            }
        }
        if (this.loadUrls.isEmpty()) {
            throw new GeaFlowDSLException("Doris fenodes must not be empty.");
        }
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
            (username + ":" + password).getBytes(StandardCharsets.UTF_8));
        this.format = format;
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
        this.columns = String.join(DorisConstants.COMMA, columns);
        // Retry at least once per FE so a single FE failure can fail over to another FE.
        this.maxRetries = Math.max(Math.max(1, maxRetries), this.loadUrls.size());
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(connectTimeoutMs)
            .setSocketTimeout(readTimeoutMs)
            .setConnectionRequestTimeout(connectTimeoutMs)
            .build();
        // The FE returns a 307 redirect that must be followed with the same method and body,
        // so mark every method as redirectable.
        this.httpClient = HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            })
            .build();
    }

    private static String normalizeFeNode(String feNode) {
        String node = feNode.trim();
        if (!node.startsWith(DorisConstants.HTTP_SCHEME) && !node.startsWith("https://")) {
            node = DorisConstants.HTTP_SCHEME + node;
        }
        return node;
    }

    /**
     * Load the given payload into Doris via a single Stream Load request. The method retries on
     * transient failures and throws a {@link GeaFlowDSLException} once all retries are exhausted.
     */
    public void load(byte[] payload) {
        Exception lastError = null;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            // Rotate over the FE list so a failed request fails over to the next FE.
            String url = loadUrls.get(attempt % loadUrls.size());
            try {
                doLoad(url, payload);
                return;
            } catch (Exception e) {
                lastError = e;
                LOGGER.warn("Stream Load attempt {}/{} to {} failed: {}", attempt + 1, maxRetries,
                    url, e.getMessage());
            }
        }
        throw new GeaFlowDSLException("Doris Stream Load failed after " + maxRetries
            + " attempts.", lastError);
    }

    private void doLoad(String loadUrl, byte[] payload) throws IOException {
        HttpPut put = new HttpPut(loadUrl);
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        put.setHeader("format", format);
        put.setHeader("label", generateLabel());
        put.setHeader("two_phase_commit", "false");
        if (DorisConstants.FORMAT_CSV.equalsIgnoreCase(format)) {
            put.setHeader("column_separator", columnSeparator);
            put.setHeader("line_delimiter", lineDelimiter);
        } else if (DorisConstants.FORMAT_JSON.equalsIgnoreCase(format)) {
            put.setHeader("strip_outer_array", "true");
            put.setHeader("read_json_by_line", "false");
        }
        if (columns != null && !columns.isEmpty()) {
            put.setHeader("columns", columns);
        }
        put.setEntity(new ByteArrayEntity(payload));

        try (CloseableHttpResponse response = httpClient.execute(put)) {
            int statusCode = response.getStatusLine().getStatusCode();
            String body = response.getEntity() == null ? ""
                : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (statusCode != 200) {
                throw new IOException("Stream Load http status: " + statusCode + ", body: " + body);
            }
            checkLoadResult(body);
        }
    }

    private void checkLoadResult(String body) throws IOException {
        JsonObject result = gson.fromJson(body, JsonObject.class);
        if (!result.has(DorisConstants.STREAM_LOAD_RESULT_STATUS)) {
            throw new IOException("Stream Load response without status: " + body);
        }
        String status = result.get(DorisConstants.STREAM_LOAD_RESULT_STATUS).getAsString();
        if (!DorisConstants.STREAM_LOAD_SUCCESS.equals(status)
            && !DorisConstants.STREAM_LOAD_PUBLISH_TIMEOUT.equals(status)) {
            String message = result.has(DorisConstants.STREAM_LOAD_RESULT_MESSAGE)
                ? result.get(DorisConstants.STREAM_LOAD_RESULT_MESSAGE).getAsString() : body;
            throw new IOException("Stream Load failed, status: " + status + ", message: " + message);
        }
    }

    private String generateLabel() {
        return "geaflow_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString()
            .replace("-", "");
    }

    public String getLoadUrl() {
        return loadUrls.get(0);
    }

    public List<String> getLoadUrls() {
        return loadUrls;
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
