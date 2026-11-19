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

package org.apache.geaflow.ai.temporal.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TemporalRetrievalBaselineCliTest {

    @Test
    public void testRejectsMissingUnknownAndDuplicateOptionsWithoutStartingInference() throws Exception {
        rejected();
        rejected("--output", "unused");
        rejected("--module-root", "unused", "--output", "unused", "--unknown", "value");
        rejected("--module-root", "unused", "--output", "unused", "--output", "duplicate");
    }

    @Test
    public void testRejectsInvalidOfflineValueWithoutStartingInference() throws Exception {
        rejected("--module-root", "unused", "--output", "unused", "--offline", "maybe");
    }

    private static void rejected(String... arguments) throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final ByteArrayOutputStream errors = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8.name());
             PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8.name())) {
            Assertions.assertEquals(2, new TemporalRetrievalBaselineCli().run(arguments, out, err));
        }
        Assertions.assertEquals(0, output.size());
        Assertions.assertEquals("REJECT_INPUT", new TemporalJsonCodec().parseObject(
            new String(errors.toByteArray(), StandardCharsets.UTF_8)).get("code").getAsString());
    }
}
