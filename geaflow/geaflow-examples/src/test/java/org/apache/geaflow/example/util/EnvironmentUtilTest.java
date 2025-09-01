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

package org.apache.geaflow.example.util;

import static org.apache.geaflow.cluster.constants.ClusterConstants.CLUSTER_TYPE;
import static org.apache.geaflow.cluster.constants.ClusterConstants.LOCAL_CLUSTER;

import org.apache.geaflow.cluster.local.client.LocalEnvironment;
import org.apache.geaflow.env.Environment;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EnvironmentUtilTest {

    @Test
    public void test() {
        Environment env = EnvironmentUtil.loadEnvironment(null);
        Assert.assertTrue(env instanceof LocalEnvironment);

        System.setProperty(CLUSTER_TYPE, LOCAL_CLUSTER);
        env = EnvironmentUtil.loadEnvironment(null);
        Assert.assertTrue(env instanceof LocalEnvironment);
    }

}
