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

package org.apache.geaflow.cluster.k8s.client;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import com.alibaba.fastjson.JSON;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class KubernetesJobSubmitterTest {

    @Test(expectedExceptions = NoSuchMethodException.class)
    public void testSubmit() throws Throwable {
        KubernetesJobSubmitter submitter = new KubernetesJobSubmitter();
        String[] args = new String[]{"start", this.getClass().getCanonicalName(), "{}"};
        submitter.submitJob(args);
    }

    @Test(expectedExceptions = KubernetesClientException.class)
    public void testStop() throws Throwable {
        KubernetesJobSubmitter submitter = new KubernetesJobSubmitter();
        Map<String, String> jobConfig = new HashMap<>();
        jobConfig.put(CLUSTER_ID.getKey(), "124");
        Map<String, Map<String, String>> config = new HashMap<>();
        config.put("job", jobConfig);
        String[] args = new String[]{"stop", JSON.toJSONString(config)};
        submitter.stopJob(args);
    }

}
