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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleClusterStartedCallback implements ClusterStartedCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleClusterStartedCallback.class);

    @Override
    public void onSuccess(ClusterMeta clusterInfo) {
        LOGGER.info("start cluster successfully: {}", clusterInfo);
    }

    @Override
    public void onFailure(Throwable e) {
        LOGGER.error("start cluster failed", e);
    }

}
