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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;

import com.google.gson.Gson;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.utils.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestJobOperatorCallback implements JobOperatorCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestJobOperatorCallback.class);
    private static final String GEAFLOW_TOKEN_KEY = "geaflow-token";
    private static final String FINISH_JOB_PATH = "/api/tasks/%s/operations";
    private static final String FINISH_ACTION_KEY = "finish";
    private static final String JOB_ACTION_KEY = "action";

    private final String callbackUrl;
    private final Map<String, String> headers;
    private final long uniqueId;

    public RestJobOperatorCallback(Configuration config, String url) {
        this.uniqueId = config.getLong(JOB_UNIQUE_ID);
        this.callbackUrl = url;
        this.headers = new HashMap<>();
        this.headers.put(GEAFLOW_TOKEN_KEY, config.getString(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY, ""));
    }

    @Override
    public void onFinish() {
        Map<String, String> params = new HashMap<>();
        params.put(JOB_ACTION_KEY, FINISH_ACTION_KEY);
        JobOperatorCallback.JobOperatorMeta jobOperatorMeta = new JobOperatorCallback.JobOperatorMeta(params);

        String fullUrl = getFullUrl(jobOperatorMeta);
        if (fullUrl != null) {
            HttpRequest request = new HttpRequest();
            request.setSuccess(true);
            request.setData(jobOperatorMeta);
            HttpUtil.post(fullUrl, new Gson().toJson(request), headers);
        }
    }

    private String getFullUrl(JobOperatorMeta jobOperatorMeta) {
        String fullUrl = null;
        try {
            URI uri = new URI(this.callbackUrl);
            String path = String.format(FINISH_JOB_PATH, uniqueId);
            fullUrl = uri.resolve(path).toString();
        } catch (URISyntaxException e) {
            LOGGER.error("post {} failed: {}, msg: {}", fullUrl, jobOperatorMeta, e.getMessage());
        }
        return fullUrl;
    }

}
