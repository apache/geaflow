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

package org.apache.geaflow.analytics.service.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AnalyticsServiceInfoTest {

    @Test
    public void testAnalyticsServiceInfoWithServerName() {
        List<HostAndPort> addresses = Arrays.asList(
            new HostAndPort("host1", 8080),
            new HostAndPort("host2", 8081)
        );

        AnalyticsServiceInfo serviceInfo = new AnalyticsServiceInfo("testServer", addresses);

        Assert.assertEquals(serviceInfo.getServerName(), "testServer");
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(), addresses);
        Assert.assertEquals(serviceInfo.getCoordinatorNum(), 2);
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(0), addresses.get(0));
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(1), addresses.get(1));
    }

    @Test
    public void testAnalyticsServiceInfoWithoutServerName() {
        List<HostAndPort> addresses = Collections.singletonList(
            new HostAndPort("localhost", 9090)
        );

        AnalyticsServiceInfo serviceInfo = new AnalyticsServiceInfo(addresses);

        Assert.assertNull(serviceInfo.getServerName());
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(), addresses);
        Assert.assertEquals(serviceInfo.getCoordinatorNum(), 1);
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(0), addresses.get(0));
    }

    @Test
    public void testGetCoordinatorAddressByIndex() {
        List<HostAndPort> addresses = Arrays.asList(
            new HostAndPort("host1", 8080),
            new HostAndPort("host2", 8081),
            new HostAndPort("host3", 8082)
        );

        AnalyticsServiceInfo serviceInfo = new AnalyticsServiceInfo(addresses);

        Assert.assertEquals(serviceInfo.getCoordinatorNum(), 3);
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(0).getHost(), "host1");
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(0).getPort(), 8080);
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(1).getHost(), "host2");
        Assert.assertEquals(serviceInfo.getCoordinatorAddresses(2).getHost(), "host3");
    }

    @Test
    public void testEmptyCoordinatorAddresses() {
        List<HostAndPort> addresses = Collections.emptyList();
        AnalyticsServiceInfo serviceInfo = new AnalyticsServiceInfo(addresses);

        Assert.assertEquals(serviceInfo.getCoordinatorNum(), 0);
        Assert.assertTrue(serviceInfo.getCoordinatorAddresses().isEmpty());
    }
}
