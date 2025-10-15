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

package org.apache.geaflow.cluster.client.callback;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.utils.HttpUtil;

public class RestClusterStartedCallback implements ClusterStartedCallback {

    private static final String GEAFLOW_TOKEN_KEY = "geaflow-token";

    private final String callbackUrl;

    private final Map<String, String> headers;

    public RestClusterStartedCallback(Configuration config, String url) {
        this.callbackUrl = url;
        this.headers = new HashMap<>();
        this.headers.put(GEAFLOW_TOKEN_KEY, config.getString(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY, ""));
    }

    @Override
    public void onSuccess(ClusterMeta clusterInfo) {
        HttpRequest request = new HttpRequest();
        request.setSuccess(true);
        request.setData(clusterInfo);
        HttpUtil.post(callbackUrl, new Gson().toJson(request), headers);
    }

    @Override
    public void onFailure(Throwable e) {
        HttpRequest request = new HttpRequest();
        request.setSuccess(false);
        request.setMessage(e.getMessage());
        HttpUtil.post(callbackUrl, new Gson().toJson(request), headers);
    }

}
