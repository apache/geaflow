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

package org.apache.geaflow.cluster.web.agent;

import com.alibaba.fastjson.JSON;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.geaflow.cluster.web.api.ApiResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

public class AgentWebServerTest {

    private static final int AGENT_PORT = 8088;

    private static final String LOG_DIR = "/tmp/geaflow/test/logs/geaflow";

    private static final String CLIENT_LOG_PATH = LOG_DIR + File.separator + "client.log";

    private static final String FLAME_GRAPH_PROFILER_PATH = "/tmp/async-profiler/profiler.sh";

    private static final String FLAME_GRAPH_PROFILER_FILENAME_EXTENSION = ".html";

    private static final String AGENT_DIR = "/tmp/agent";

    private static final String THREAD_DUMP_LOG_FILE_PATH = "/tmp/agent/geaflow-thread-dump.log";

    @Test
    public void testServer() throws Exception {
        AgentWebServer httpServer = new AgentWebServer(AGENT_PORT, LOG_DIR,
            FLAME_GRAPH_PROFILER_PATH, FLAME_GRAPH_PROFILER_FILENAME_EXTENSION, AGENT_DIR);
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                httpServer.start();
                latch.countDown();
            }
        }).start();

        latch.await();
        doGet("http://localhost:8088/", "rest/logs");
        doGet("http://localhost:8088/",
            "rest/logs/content?path=/tmp/geaflow/test/logs/geaflow/client.log&pageNo=1&pageSize=1024");
        doGet("http://localhost:8088/",
            "rest/thread-dump/content?pageNo=1&pageSize=1024");
        httpServer.stop();
    }

    @BeforeClass
    public static void init() {
        initLogFiles();
    }

    @AfterClass
    public static void afterClass() {
        cleanup();
    }

    private static void initLogFiles() {
        initLogFile(CLIENT_LOG_PATH);
        initLogFile(THREAD_DUMP_LOG_FILE_PATH);
    }

    private static void initLogFile(String filePath) {
        try {
            File file = new File(filePath);
            File fileParent = file.getParentFile();
            if (!fileParent.exists()) {
                fileParent.mkdirs();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(file);
            for (int i = 0; i < 10240; i++) {
                fos.write(("Mock log " + i + "\n").getBytes());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void cleanup() {
        cleanLogFile(CLIENT_LOG_PATH);
        cleanLogFile(THREAD_DUMP_LOG_FILE_PATH);
    }

    private static void cleanLogFile(String filePath) {
        File file = new File(filePath);
        if (file.exists() && file.isFile()) {
            file.delete();
        }
    }

    private void doGet(String url, String path) throws Exception {
        URI uri = new URI(url);
        String fullUrl = uri.resolve(path).toString();
        Request request = new Request.Builder().url(fullUrl).get().build();

        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            Assert.assertEquals(response.code(), 200);
            ApiResponse apiResponse = JSON.parseObject(responseBody.string(), ApiResponse.class);
            Assert.assertTrue(apiResponse.isSuccess());
        }
    }


}
