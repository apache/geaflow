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

package org.apache.geaflow.analytics.service.client.jdbc;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AnalyticsDriverURITest {

    @Test
    public void testValidURLWithUserAndGraphView() {
        String url = "jdbc:geaflow://localhost:8080/myGraph?user=testUser";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        
        Assert.assertEquals(driverURI.getGraphView(), "myGraph");
        Assert.assertEquals(driverURI.getUser(), "testUser");
        Assert.assertEquals(driverURI.getAuthority(), "localhost:8080");
        Assert.assertFalse(driverURI.isCompressionDisabled());
    }

    @Test
    public void testValidURLWithoutGraphView() {
        String url = "jdbc:geaflow://localhost:8080?user=testUser";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        
        Assert.assertNull(driverURI.getGraphView());
        Assert.assertEquals(driverURI.getUser(), "testUser");
    }

    @Test
    public void testUserInProperties() {
        String url = "jdbc:geaflow://localhost:8080";
        Properties props = new Properties();
        props.setProperty("user", "propUser");
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        
        Assert.assertEquals(driverURI.getUser(), "propUser");
    }

    @Test
    public void testURLParametersParsing() {
        String url = "jdbc:geaflow://localhost:8080?user=testUser&timeout=5000";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        
        Properties properties = driverURI.getProperties();
        Assert.assertEquals(properties.getProperty("user"), "testUser");
        Assert.assertEquals(properties.getProperty("timeout"), "5000");
    }

    @Test
    public void testHttpURI() {
        String url = "jdbc:geaflow://localhost:8080?user=testUser";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        URI httpUri = driverURI.getHttpUri();
        
        Assert.assertEquals(httpUri.getScheme(), "http");
        Assert.assertEquals(httpUri.getHost(), "localhost");
        Assert.assertEquals(httpUri.getPort(), 8080);
    }

    @Test
    public void testSecureHttpURI() {
        String url = "jdbc:geaflow://localhost:443?user=testUser";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        URI httpUri = driverURI.getHttpUri();
        
        Assert.assertEquals(httpUri.getScheme(), "https");
        Assert.assertEquals(httpUri.getPort(), 443);
    }

    @Test
    public void testGetSessionProperties() {
        String url = "jdbc:geaflow://localhost:8080?user=testUser";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        Map<String, String> sessionProps = driverURI.getSessionProperties();
        
        Assert.assertNotNull(sessionProps);
    }

    @Test
    public void testGetCustomHeaders() {
        String url = "jdbc:geaflow://localhost:8080?user=testUser";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        Map<String, String> customHeaders = driverURI.getCustomHeaders();
        
        Assert.assertNotNull(customHeaders);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testInvalidURLWithoutHost() {
        String url = "jdbc:geaflow://:8080?user=testUser";
        Properties props = new Properties();
        
        new AnalyticsDriverURI(url, props);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testInvalidURLWithoutPort() {
        String url = "jdbc:geaflow://localhost?user=testUser";
        Properties props = new Properties();
        
        new AnalyticsDriverURI(url, props);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testInvalidURLWithInvalidPort() {
        String url = "jdbc:geaflow://localhost:99999?user=testUser";
        Properties props = new Properties();
        
        new AnalyticsDriverURI(url, props);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testInvalidURLWithZeroPort() {
        String url = "jdbc:geaflow://localhost:0?user=testUser";
        Properties props = new Properties();
        
        new AnalyticsDriverURI(url, props);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testURLWithoutUser() {
        String url = "jdbc:geaflow://localhost:8080";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        driverURI.getUser(); // Should throw exception
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testDuplicatePropertyInURLAndProperties() {
        String url = "jdbc:geaflow://localhost:8080?user=urlUser";
        Properties props = new Properties();
        props.setProperty("user", "propUser");
        
        new AnalyticsDriverURI(url, props);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testDuplicateParameterInURL() {
        String url = "jdbc:geaflow://localhost:8080?user=user1&user=user2";
        Properties props = new Properties();
        
        new AnalyticsDriverURI(url, props);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testInvalidGraphViewPath() {
        String url = "jdbc:geaflow://localhost:8080/graph/extra/segment?user=testUser";
        Properties props = new Properties();
        
        new AnalyticsDriverURI(url, props);
    }

    @Test
    public void testEmptyGraphView() {
        String url = "jdbc:geaflow://localhost:8080/?user=testUser";
        Properties props = new Properties();
        
        // Empty path after '/' is treated as no graph view, which is valid
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        Assert.assertNull(driverURI.getGraphView());
    }

    @Test
    public void testGraphViewWithTrailingSlash() {
        String url = "jdbc:geaflow://localhost:8080/myGraph/?user=testUser";
        Properties props = new Properties();
        
        AnalyticsDriverURI driverURI = new AnalyticsDriverURI(url, props);
        
        Assert.assertEquals(driverURI.getGraphView(), "myGraph");
    }
}
