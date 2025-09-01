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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.GEAFLOW_GW_ENDPOINT;

import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;

public class JobOperatorCallbackFactory {

    public static JobOperatorCallback createJobOperatorCallback(Configuration configuration) {
        String callbackUrl = configuration.getString(GEAFLOW_GW_ENDPOINT, "");
        if (StringUtils.isNotBlank(callbackUrl)) {
            return new RestJobOperatorCallback(configuration, callbackUrl);
        }
        return new SimpleJobOperatorCallback();
    }

}
