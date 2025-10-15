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

package org.apache.geaflow.cluster.ray;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.geaflow.utils.HttpUtil;
import org.testng.annotations.Test;

public class RayJobTest {

    @Test(enabled = false)
    public void testRayJobSubmitTest() {
        String mainClass = "org.apache.geaflow.GeaFlowJobDemo";
        List<String> remoteJarUrls = Arrays.asList(
            "https:://public-engine-url-geaflow-0.6.zip",
            "https://public-udf-url-udf.zip");
        String args = "{\"job\":{\"geaflow.config.key\":\"true\"}";
        args = StringEscapeUtils.escapeJava(args);
        args = StringEscapeUtils.escapeJava("\"" + args + "\"");

        submitJob(remoteJarUrls, mainClass, args);
    }

    public static void submitJob(List<String> remoteJarUrls, String mainClass, String args) {
        // Cluster args.
        // ray dashboard url.
        String rayDashboardAddress = "127.0.0.1:8265";
        // ray redis url.
        String rayRedisAddress = "127.0.0.1:6379";
        String rayDistJarPath = "path-to-ray-cluster/ray_dist.jar";
        String raySessionResourceJarPath = "/path-to-ray-cluster-session-dir/session_latest/runtime_resources/java_jars_files/";

        // Job args.
        List<String> downloadJarPaths = new ArrayList<>(remoteJarUrls.size());
        for (String remoteJarUrl : remoteJarUrls) {
            String str = remoteJarUrl.replace(".zip", "");
            String result = str.replaceAll("[:/.]+", "_");
            downloadJarPaths.add(raySessionResourceJarPath + result + "/*");
        }

        String downloadJarClassPath = String.join(":", downloadJarPaths);
        List<String> remoteJarUrlsStr = new ArrayList<>(remoteJarUrls.size());
        for (String remoteUrl : remoteJarUrls) {
            remoteJarUrlsStr.add("\"" + remoteUrl + "\"");
        }
        String remoteJarJsonPath = String.join(",", remoteJarUrlsStr);

        String request = String.format("{\n"
            + "\"entrypoint\": \"java -classpath %s:%s -Dlog.file=/tmp/logfile.log  -Dray.address=%s %s %s\",\n"
            + "\"runtime_env\": {\"java_jars\": [%s]}\n"
            + "}", rayDistJarPath, downloadJarClassPath, rayRedisAddress, mainClass, args, remoteJarJsonPath);

        String postUrl = String.format("http://%s/api/jobs/", rayDashboardAddress);
        System.out.println("request: \n" + request + "\npost url: \n" + postUrl);
        HttpUtil.post(postUrl, request);
    }
}
