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

package org.apache.geaflow.utils;

import com.google.gson.Gson;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.Callable;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.utils.client.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);
    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final Gson GSON = new Gson();
    private static final int DEFAULT_RETRY_TIMES = 3;

    public static Object post(String url, String json) {
        return post(url, json, Object.class);
    }

    public static Object post(String url, String json, Map<String, String> headers) {
        return post(url, json, headers, Object.class);
    }

    public static <T> T post(String url, String json, Class<T> resultClass) {
        return post(url, json, null, resultClass);
    }

    public static <T> T post(String url, String body, Map<String, String> headers,
                             Class<T> resultClass) {
        LOGGER.info("post url: {} body: {}", url, body);
        RequestBody requestBody = RequestBody.create(MEDIA_TYPE, body);
        Builder builder = getRequestBuilder(url, headers);
        Request request = builder.post(requestBody).build();

        long t = System.currentTimeMillis();
        OkHttpClient client = new OkHttpClient();
        return RetryCommand.run(new Callable<T>() {
            @Override
            public T call() throws Exception {
                try (Response response = client.newCall(request).execute()) {
                    ResponseBody responseBody = response.body();
                    String msg = (responseBody != null) ? responseBody.string() : "{}";
                    if (!response.isSuccessful()) {
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
                    }
                    HttpResponse httpResponse = GSON.fromJson(msg, HttpResponse.class);
                    if (!httpResponse.isSuccess()) {
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
                    }
                    T result = GSON.fromJson(httpResponse.getData(), resultClass);
                    LOGGER.info("post {} response cost {}ms: {}", url, System.currentTimeMillis() - t, msg);
                    return result;
                } catch (IOException e) {
                    LOGGER.info("execute post failed: {}", e.getCause(), e);
                    throw new GeaflowRuntimeException(e);
                }
            }
        }, DEFAULT_RETRY_TIMES);
    }

    public static <T> T get(String url, Map<String, String> headers, Class<T> resultClass) {
        return get(url, headers, (Type) resultClass);
    }

    public static <T> T get(String url, Map<String, String> headers, Type typeOfT) {
        LOGGER.info("get url: {}", url);
        Builder builder = getRequestBuilder(url, headers);
        Request request = builder.get().build();

        long t = System.currentTimeMillis();
        OkHttpClient client = new OkHttpClient();
        return RetryCommand.run(new Callable<T>() {
            @Override
            public T call() throws Exception {
                try (Response response = client.newCall(request).execute()) {
                    ResponseBody responseBody = response.body();
                    String msg = (responseBody != null) ? responseBody.string() : "{}";
                    if (!response.isSuccessful()) {
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
                    }
                    HttpResponse httpResponse = GSON.fromJson(msg, HttpResponse.class);
                    if (!httpResponse.isSuccess()) {
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
                    }
                    T result = GSON.fromJson(httpResponse.getData(), typeOfT);
                    LOGGER.info("get {} response cost {}ms: {}", url, System.currentTimeMillis() - t,
                        msg);
                    return result;
                } catch (IOException e) {
                    LOGGER.info("execute get failed: {}", e.getCause(), e);
                    throw new GeaflowRuntimeException(e);
                }
            }
        }, DEFAULT_RETRY_TIMES);
    }

    public static boolean delete(String url) {
        return delete(url, null);
    }

    public static boolean delete(String url, Map<String, String> headers) {
        LOGGER.info("delete url: {}", url);
        OkHttpClient client = new OkHttpClient();
        Builder requestBuilder = getRequestBuilder(url, headers);
        Request request = requestBuilder.delete().build();
        long t = System.currentTimeMillis();
        return RetryCommand.run(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try (Response response = client.newCall(request).execute()) {
                    ResponseBody body = response.body();
                    String msg = (body != null) ? body.string() : "{}";
                    if (!response.isSuccessful()) {
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
                    } else {
                        LOGGER.info("delete {} cost {}ms", url, System.currentTimeMillis() - t);
                        return true;
                    }
                } catch (IOException e) {
                    LOGGER.info("execute delete failed: {}", e.getCause(), e);
                    throw new GeaflowRuntimeException(e);
                }
            }
        }, DEFAULT_RETRY_TIMES);
    }

    private static Builder getRequestBuilder(String url, Map<String, String> headers) {
        Builder requestBuilder = new Request.Builder().url(url);
        if (headers != null) {
            Headers requestHeaders = Headers.of(headers);
            requestBuilder.headers(requestHeaders);
        }
        return requestBuilder;
    }

}
