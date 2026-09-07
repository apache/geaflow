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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.ai.retrieval.api;

import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Configuration defaults and hard-limit tests. */
public class RetrievalPropertiesTest {

    @Test
    public void validatesVersionedDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.validateConfiguration();

        Assertions.assertEquals("v1", properties.getConfigVersion());
        Assertions.assertEquals("Confucius", properties.getReadyGraphName());
        Assertions.assertEquals("KEYWORD", properties.getDefaultMode());
        Assertions.assertEquals("SEQUENTIAL", properties.getDefaultExecutionMode());
        Assertions.assertEquals(10, properties.getDefaultTopK());
        Assertions.assertEquals(100, properties.getMaxTopK());
        Assertions.assertEquals(3000, properties.getDefaultTimeoutMs());
        Assertions.assertEquals(10000, properties.getMaxTimeoutMs());
        Assertions.assertEquals(100, properties.getDefaultMaxCandidates());
        Assertions.assertEquals(1000, properties.getMaxCandidates());
        Assertions.assertEquals(4096, properties.getDefaultTokenBudget());
        Assertions.assertEquals(16384, properties.getMaxTokenBudget());
    }

    @Test
    public void rejectsInvalidModesAndLimits() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setDefaultMode("HYBRID");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultExecutionMode("PARALLEL");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultTimeoutMs(10001);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultTokenBudget(16385);
        assertInvalid(properties);
    }

    @Test
    public void rejectsBlankIdentityAndInvertedDefaults() {
        RetrievalProperties properties = new RetrievalProperties();
        properties.setConfigVersion(" ");
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setReadyGraphName(null);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setDefaultTopK(10);
        properties.setDefaultMaxCandidates(9);
        assertInvalid(properties);

        properties = new RetrievalProperties();
        properties.setMaxTopK(0);
        assertInvalid(properties);
    }

    private static void assertInvalid(RetrievalProperties properties) {
        RetrievalException exception = Assertions.assertThrows(RetrievalException.class,
            properties::validateConfiguration);
        Assertions.assertEquals(RetrievalErrorCode.INVALID_REQUEST, exception.getCode());
    }
}
