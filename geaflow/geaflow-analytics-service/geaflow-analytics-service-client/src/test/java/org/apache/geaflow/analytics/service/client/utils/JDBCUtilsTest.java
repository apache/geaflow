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

package org.apache.geaflow.analytics.service.client.utils;

import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JDBCUtilsTest {

    @Test
    public void testAcceptsValidURL() {
        String validUrl = "jdbc:geaflow://localhost:8080";
        Assert.assertTrue(JDBCUtils.acceptsURL(validUrl));
    }

    @Test
    public void testAcceptsValidURLWithPath() {
        String validUrl = "jdbc:geaflow://localhost:8080/graphView";
        Assert.assertTrue(JDBCUtils.acceptsURL(validUrl));
    }

    @Test
    public void testAcceptsValidURLWithParameters() {
        String validUrl = "jdbc:geaflow://localhost:8080?user=test&password=test123";
        Assert.assertTrue(JDBCUtils.acceptsURL(validUrl));
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testRejectsInvalidURL() {
        String invalidUrl = "jdbc:mysql://localhost:3306/test";
        JDBCUtils.acceptsURL(invalidUrl);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testRejectsEmptyURL() {
        JDBCUtils.acceptsURL("");
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testRejectsNonJDBCURL() {
        String nonJdbcUrl = "http://localhost:8080";
        JDBCUtils.acceptsURL(nonJdbcUrl);
    }

    @Test
    public void testDriverURLStartConstant() {
        Assert.assertEquals(JDBCUtils.DRIVER_URL_START, "jdbc:geaflow://");
    }
}
