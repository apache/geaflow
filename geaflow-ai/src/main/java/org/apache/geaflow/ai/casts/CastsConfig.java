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

package org.apache.geaflow.ai.casts;

public class CastsConfig {

    public final String baseUrl;
    public final String token;

    public CastsConfig(String baseUrl, String token) {
        this.baseUrl = baseUrl;
        this.token = token == null ? "" : token;
    }

    public static CastsConfig fromEnv() {
        String url = System.getenv("GEAFLOW_AI_CASTS_URL");
        if (url == null || url.isEmpty()) {
            url = "http://localhost:5001";
        }
        String token = System.getenv("GEAFLOW_AI_CASTS_TOKEN");
        return new CastsConfig(url, token);
    }

    public String decisionUrl() {
        return join(baseUrl, "/casts/decision");
    }

    private static String join(String base, String path) {
        if (base.endsWith("/") && path.startsWith("/")) {
            return base.substring(0, base.length() - 1) + path;
        }
        if (!base.endsWith("/") && !path.startsWith("/")) {
            return base + "/" + path;
        }
        return base + path;
    }
}

