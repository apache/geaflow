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

import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.util.Properties;
import org.testng.annotations.Test;

public class AnalyticsDriverURITest {

    @Test
    public void testInvalidURI() {
        assertInvalid("jdbc:geaflow://localhost/", "No port number specified:");
    }

    private static void assertInvalid(String url, String prefix) {
        try {
            createDriverUri(url);
            fail("expected exception");
        } catch (Exception e) {
            assertNotNull(e.getMessage());
            if (!e.getMessage().startsWith(prefix)) {
                fail(format("expected:<%s> to start with <%s>", e.getMessage(), prefix));
            }
        }
    }

    private static AnalyticsDriverURI createDriverUri(String url) {
        Properties properties = new Properties();
        properties.setProperty("user", "only-test");
        return new AnalyticsDriverURI(url, properties);
    }
}
