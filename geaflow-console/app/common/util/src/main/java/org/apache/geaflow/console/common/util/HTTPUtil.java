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

package org.apache.geaflow.console.common.util;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTTPUtil.class);
    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");

    public static JSONObject post(String url, String json) {
        return post(url, json, JSONObject.class);
    }

    public static JSONObject post(String url, String json, Map<String, String> headers) {
        return post(url, json, headers, JSONObject.class);
    }

    public static <T> T post(String url, String json, Class<T> resultClass) {
        return post(url, json, null, resultClass);
    }

    public static <T> T post(String url, String body, Map<String, String> headers, Class<T> resultClass) {
        LOGGER.info("post url: {} body: {}", url, body);
        RequestBody requestBody = RequestBody.create(body, MEDIA_TYPE);
        Builder builder = getRequestBuilder(url, headers);
        Request request = builder.post(requestBody).build();

        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            String msg = (responseBody != null) ? responseBody.string() : "{}";
            if (!response.isSuccessful()) {
                throw new RuntimeException(msg);
            }

            return JSONObject.toJavaObject(JSONObject.parseObject(msg), resultClass);
        } catch (IOException e) {
            LOGGER.info("execute post failed: {}", e.getCause(), e);
            throw new RuntimeException(e);
        }
    }


    public static JSONObject get(String url) {
        return get(url, null, JSONObject.class);
    }


    public static <T> T get(String url, Map<String, String> headers, Class<T> resultClass) {
        LOGGER.info("get url: {}", url);
        Builder builder = getRequestBuilder(url, headers);
        Request request = builder.get().build();


        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            String msg = (responseBody != null) ? responseBody.string() : "{}";
            if (!response.isSuccessful()) {
                throw new RuntimeException(msg);
            }

            return JSONObject.toJavaObject(JSONObject.parseObject(msg), resultClass);
        } catch (IOException e) {
            LOGGER.info("execute get failed: {}", e.getCause(), e);
            throw new RuntimeException(e);
        }
    }

    private static Builder getRequestBuilder(String url, Map<String, String> headers) {
        Builder requestBuilder = new Request.Builder().url(url);
        if (headers != null) {
            Headers requestHeaders = Headers.of(headers);
            requestBuilder.headers(requestHeaders);
        }
        return requestBuilder;
    }

    public static void download(HttpServletResponse response, InputStream inputStream, String fileName)
        throws IOException {
        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);

        try (ServletOutputStream output = response.getOutputStream()) {
            IOUtils.copy(inputStream, output, 1024 * 1024 * 8);
            output.flush();
        }
    }
}
