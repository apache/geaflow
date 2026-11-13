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

package org.apache.geaflow.dsl.connector.clickhouse;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class ClickHouseTestEnvironmentTest {

    @Test
    public void resolvesLocalAndCiDefaultsAndExplicitOverrides() {
        Assert.assertFalse(ClickHouseTestEnvironment.isDockerRequired(null, null));
        Assert.assertFalse(ClickHouseTestEnvironment.isDockerRequired(null, ""));
        Assert.assertFalse(ClickHouseTestEnvironment.isDockerRequired(null, "false"));
        Assert.assertFalse(ClickHouseTestEnvironment.isDockerRequired(null, "0"));
        Assert.assertTrue(ClickHouseTestEnvironment.isDockerRequired(null, "true"));
        Assert.assertTrue(ClickHouseTestEnvironment.isDockerRequired(null, "1"));
        Assert.assertTrue(ClickHouseTestEnvironment.isDockerRequired("true", null));
        Assert.assertFalse(ClickHouseTestEnvironment.isDockerRequired("false", "true"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void malformedRequiredFlagCannotSilentlyDisableTests() {
        ClickHouseTestEnvironment.isDockerRequired("tru", "true");
    }

    @Test
    public void availableDockerRunsInBothModes() {
        ClickHouseTestEnvironment.requireDocker(false, () -> true);
        ClickHouseTestEnvironment.requireDocker(true, () -> true);
    }

    @Test(expectedExceptions = SkipException.class)
    public void unavailableDockerSkipsOnlyOptionalLocalTests() {
        ClickHouseTestEnvironment.requireDocker(false, () -> false);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void unavailableRequiredDockerFails() {
        ClickHouseTestEnvironment.requireDocker(true, () -> false);
    }

    @Test
    public void discoveryErrorsFailRequiredTestsWithOriginalCause() {
        RuntimeException failure = new IllegalStateException("Docker client API incompatible");
        try {
            ClickHouseTestEnvironment.requireDocker(true, () -> {
                throw failure;
            });
            Assert.fail("A required Docker discovery error must fail the test");
        } catch (IllegalStateException error) {
            Assert.assertSame(error.getCause(), failure);
        }
    }

    @Test
    public void discoveryErrorsSkipOptionalTestsWithOriginalCause() {
        RuntimeException failure = new IllegalStateException("Docker socket unavailable");
        try {
            ClickHouseTestEnvironment.requireDocker(false, () -> {
                throw failure;
            });
            Assert.fail("An optional test without Docker must be marked skipped");
        } catch (SkipException error) {
            Assert.assertSame(error.getCause(), failure);
        }
    }
}
