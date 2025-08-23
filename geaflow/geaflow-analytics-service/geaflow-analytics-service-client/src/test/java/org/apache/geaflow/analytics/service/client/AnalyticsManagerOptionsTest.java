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

import org.testng.Assert;
import org.testng.annotations.Test;

public class AnalyticsManagerOptionsTest {
    private static final String URL_PATH = "/rest/analytics/query/execute";

    @Test
    public void testCreateClientSession() {
        String host = "localhost";
        int port = 8080;
        AnalyticsManagerSession clientSession1 = AnalyticsManagerOptions.createClientSession(host, port);
        AnalyticsManagerSession clientSession2 = AnalyticsManagerOptions.createClientSession(port);
        Assert.assertEquals(clientSession1.getServer().toString(), clientSession2.getServer().toString());
        Assert.assertEquals(clientSession1.getServer().toString(), "http://localhost:8080");
    }

    @Test
    public void testServerResolve() {
        String host = "localhost";
        int port = 8080;
        AnalyticsManagerSession clientSession = AnalyticsManagerOptions.createClientSession(host, port);
        String fullUri = clientSession.getServer().resolve(URL_PATH).toString();
        Assert.assertEquals(fullUri, "http://localhost:8080/rest/analytics/query/execute");
    }

}
