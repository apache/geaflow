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

package org.apache.geaflow.cluster.rpc;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;

import org.apache.geaflow.cluster.rpc.RpcEndpointRefFactory.EndpointType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.ha.service.HAServiceFactory;
import org.apache.geaflow.ha.service.IHAService;
import org.apache.geaflow.ha.service.ResourceData;
import org.junit.Test;
import org.testng.Assert;

public class RpcClientTest {

    @Test
    public void testInvalidateResource() {
        Configuration config = new Configuration();
        config.put(RUN_LOCAL_MODE, "true");
        RpcClient rpcClient = RpcClient.init(config);
        IHAService haService = HAServiceFactory.getService();

        ResourceData resourceData = new ResourceData();
        resourceData.setHost("host");
        resourceData.setRpcPort(2);
        haService.register("1", resourceData);

        resourceData = rpcClient.getResourceData("1");
        Assert.assertNotNull(resourceData);

        rpcClient.invalidateEndpointCache("1", EndpointType.MASTER);
        resourceData = rpcClient.getResourceData("1");
        Assert.assertNull(resourceData);
    }


}
