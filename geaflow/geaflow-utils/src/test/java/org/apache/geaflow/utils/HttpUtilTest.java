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

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class HttpUtilTest {

    MockWebServer server;
    String baseUrl;

    @BeforeTest
    public void prepare() throws IOException {
        // Create a MockWebServer.
        server = new MockWebServer();
        // Start the server.
        server.start();
        baseUrl = "http://" + server.getHostName() + ":" + server.getPort();
    }

    @AfterTest
    public void tearUp() throws IOException {
        // Shut down the server. Instances cannot be reused.
        server.shutdown();
    }

    @Test
    public void test() throws InterruptedException {
        // Schedule some responses.
        server.enqueue(new MockResponse().setBody("{key:value,success:true}"));
        server.enqueue(new MockResponse().setBody("{delete:true,success:true}"));

        // Ask the server for its URL. You'll need this to make HTTP requests.
        String url = URI.create(baseUrl).resolve("/v1/cluster").toString();
        Object result = HttpUtil.post(url, "{}");
        Assert.assertNull(result);

        // confirm that your app made the HTTP requests you were expecting.
        RecordedRequest request1 = server.takeRequest();
        Assert.assertEquals("/v1/cluster", request1.getPath());

        Map<String, String> headers = new HashMap<>();
        headers.put("header1", "value");
        Object result2 = HttpUtil.delete(url, headers);
        Assert.assertNotNull(result2);

        // confirm that your app made the HTTP requests you were expecting.
        RecordedRequest request2 = server.takeRequest();
        Assert.assertEquals("/v1/cluster", request2.getPath());
        Assert.assertEquals("value", request2.getHeaders().get("header1"));
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testPostInvalid() {
        server.enqueue(new MockResponse().setResponseCode(500));
        String invalidUrl = URI.create(baseUrl).resolve("/invalid").toString();
        HttpUtil.post(invalidUrl, "{}");
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testDeleteInvalid() {
        server.enqueue(new MockResponse().setResponseCode(500));
        String invalidUrl = URI.create(baseUrl).resolve("/invalid").toString();
        HttpUtil.delete(invalidUrl);
    }
}
