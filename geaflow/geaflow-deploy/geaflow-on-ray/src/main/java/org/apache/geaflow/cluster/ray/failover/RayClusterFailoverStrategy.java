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

import org.apache.geaflow.cluster.clustermanager.ClusterContext;
import org.apache.geaflow.cluster.ray.config.RayConfig;
import org.apache.geaflow.cluster.runner.failover.ClusterFailoverStrategy;
import org.apache.geaflow.env.IEnvironment.EnvType;

public class RayClusterFailoverStrategy extends ClusterFailoverStrategy {

    public RayClusterFailoverStrategy() {
        super(EnvType.RAY);
    }

    @Override
    public void init(ClusterContext context) {
        super.init(context);
        System.setProperty(RayConfig.RAY_TASK_RETURN_TASK_EXCEPTION, Boolean.FALSE.toString());
    }

}
