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

package org.apache.geaflow.cluster.ray.failover;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.FO_STRATEGY;

import org.apache.geaflow.cluster.failover.FailoverStrategyFactory;
import org.apache.geaflow.cluster.failover.IFailoverStrategy;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.env.IEnvironment.EnvType;
import org.junit.Assert;
import org.testng.annotations.Test;

public class RayDisableFoStrategyTest {

    @Test
    public void testLoad() {
        Configuration configuration = new Configuration();
        IFailoverStrategy foStrategy = FailoverStrategyFactory.loadFailoverStrategy(EnvType.RAY, configuration.getString(FO_STRATEGY));
        Assert.assertNotNull(foStrategy);
        Assert.assertEquals(foStrategy.getType().name(), configuration.getString(FO_STRATEGY));

        IFailoverStrategy rayDisableFoStrategy = FailoverStrategyFactory.loadFailoverStrategy(EnvType.RAY, "disable_fo");
        Assert.assertNotNull(rayDisableFoStrategy);
        Assert.assertEquals(rayDisableFoStrategy.getClass(), RayDisableFailoverStrategy.class);
    }

}
