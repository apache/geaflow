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

import java.util.Collections;
import org.apache.geaflow.ai.retrieval.api.model.ExecutionMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalBudget;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalCommand;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalErrorCode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalException;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalMode;
import org.apache.geaflow.ai.retrieval.api.model.RetrievalRequest;
import org.apache.geaflow.ai.retrieval.config.RetrievalProperties;
import org.apache.geaflow.ai.retrieval.service.RetrievalRequestValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the untrusted request to immutable command boundary. */
public class RetrievalRequestValidatorTest {

    @Test
    public void appliesDefaultsAndTrimsInput() {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName("  graph  ");
        request.setQuery("  query  ");

        RetrievalCommand command = validator().validate(request);

        Assertions.assertEquals("graph", command.getGraphName());
        Assertions.assertEquals("query", command.getQuery());
        Assertions.assertEquals(RetrievalMode.KEYWORD, command.getMode());
        Assertions.assertEquals(ExecutionMode.SEQUENTIAL, command.getExecutionMode());
        Assertions.assertEquals(Integer.valueOf(10), command.getBudget().getTopK());
        Assertions.assertEquals(Integer.valueOf(3000), command.getBudget().getTimeoutMs());
        Assertions.assertEquals(Integer.valueOf(100), command.getBudget().getMaxCandidates());
        Assertions.assertEquals(Integer.valueOf(4096), command.getBudget().getTokenBudget());
    }

    @Test
    public void acceptsInclusiveBudgetBoundaries() {
        RetrievalRequest request = request();
        request.setBudget(new RetrievalBudget(100, 10000, 1000, 16384));

        RetrievalCommand command = validator().validate(request);

        Assertions.assertEquals(Integer.valueOf(100), command.getBudget().getTopK());
        Assertions.assertEquals(Integer.valueOf(10000), command.getBudget().getTimeoutMs());
        Assertions.assertEquals(Integer.valueOf(1000), command.getBudget().getMaxCandidates());
        Assertions.assertEquals(Integer.valueOf(16384), command.getBudget().getTokenBudget());
    }

    @Test
    public void rejectsMissingAndOverlongText() {
        RetrievalRequest missingGraph = request();
        missingGraph.setGraphName(" ");
        assertCode(missingGraph, RetrievalErrorCode.INVALID_REQUEST);

        RetrievalRequest missingQuery = request();
        missingQuery.setQuery("\t");
        assertCode(missingQuery, RetrievalErrorCode.INVALID_REQUEST);

        RetrievalRequest longGraph = request();
        longGraph.setGraphName(repeat('g', 129));
        assertCode(longGraph, RetrievalErrorCode.INVALID_REQUEST);

        RetrievalRequest longQuery = request();
        longQuery.setQuery(repeat('q', 4097));
        assertCode(longQuery, RetrievalErrorCode.INVALID_REQUEST);
    }

    @Test
    public void rejectsUnsupportedModeExecutionAndVector() {
        RetrievalRequest mode = request();
        mode.setMode("HYBRID");
        assertCode(mode, RetrievalErrorCode.INVALID_REQUEST);

        RetrievalRequest execution = request();
        execution.setExecutionMode("PARALLEL");
        assertCode(execution, RetrievalErrorCode.UNSUPPORTED_OPTION);

        RetrievalRequest unknownExecution = request();
        unknownExecution.setExecutionMode("unknown");
        assertCode(unknownExecution, RetrievalErrorCode.UNSUPPORTED_OPTION);

        RetrievalRequest vector = request();
        vector.setQueryVector(Collections.singletonList(0.1));
        assertCode(vector, RetrievalErrorCode.UNSUPPORTED_OPTION);

        RetrievalRequest emptyVector = request();
        emptyVector.setQueryVector(Collections.<Double>emptyList());
        Assertions.assertDoesNotThrow(() -> validator().validate(emptyVector));
    }

    @Test
    public void acceptsHybridVectorAndCaseInsensitiveExecutionMode() {
        RetrievalRequest request = request();
        request.setMode("hybrid");
        request.setExecutionMode(" sequential ");
        request.setQueryVector(Collections.singletonList(0.1));

        RetrievalCommand command = validator().validate(request);

        Assertions.assertEquals(RetrievalMode.HYBRID, command.getMode());
        Assertions.assertEquals(ExecutionMode.SEQUENTIAL, command.getExecutionMode());
    }

    @Test
    public void rejectsVectorsForNonVectorModesAndBlankMetadata() {
        for (String mode : new String[] {"BM25_ONLY", "KEYWORD", "GRAPH_ONLY"}) {
            RetrievalRequest request = request();
            request.setMode(mode);
            request.setQueryVector(Collections.singletonList(0.1));
            assertCode(request, RetrievalErrorCode.UNSUPPORTED_OPTION);
        }
        RetrievalRequest invalid = request();
        invalid.setRequestId(" ");
        assertCode(invalid, RetrievalErrorCode.INVALID_REQUEST);
    }

    @Test
    public void rejectsBudgetRangesAndRelationships() {
        assertBudget(new RetrievalBudget(0, 3000, 100, 4096));
        assertBudget(new RetrievalBudget(101, 3000, 100, 4096));
        assertBudget(new RetrievalBudget(10, 0, 100, 4096));
        assertBudget(new RetrievalBudget(10, 10001, 100, 4096));
        assertBudget(new RetrievalBudget(10, 3000, 0, 4096));
        assertBudget(new RetrievalBudget(10, 3000, 1001, 4096));
        assertBudget(new RetrievalBudget(10, 3000, 100, 0));
        assertBudget(new RetrievalBudget(10, 3000, 100, 16385));
        assertBudget(new RetrievalBudget(101, 3000, 100, 4096));
    }

    @Test
    public void rejectsTopKGreaterThanCandidateBudget() {
        RetrievalRequest request = request();
        request.setBudget(new RetrievalBudget(20, 3000, 10, 4096));
        assertCode(request, RetrievalErrorCode.INVALID_REQUEST);
    }

    @Test
    public void commandCopiesBudgetValues() {
        RetrievalBudget budget = new RetrievalBudget(5, 100, 10, 1000);
        RetrievalRequest request = request();
        request.setBudget(budget);
        RetrievalCommand command = validator().validate(request);
        RetrievalBudget commandBudget = command.getBudget();
        commandBudget.setTopK(99);

        Assertions.assertEquals(Integer.valueOf(5), command.getBudget().getTopK());
    }

    @Test
    public void invalidPropertiesAreRejectedBeforeValidation() {
        RetrievalProperties properties = properties();
        properties.setDefaultTopK(101);
        Assertions.assertThrows(RetrievalException.class, properties::validateConfiguration);

        properties = properties();
        properties.setDefaultTopK(101);
        properties.setDefaultMaxCandidates(100);
        RetrievalException exception = Assertions.assertThrows(RetrievalException.class,
            properties::validateConfiguration);
        Assertions.assertEquals(RetrievalErrorCode.INVALID_REQUEST, exception.getCode());
    }

    private static RetrievalRequest request() {
        RetrievalRequest request = new RetrievalRequest();
        request.setGraphName("graph");
        request.setQuery("query");
        return request;
    }

    private static RetrievalRequestValidator validator() {
        RetrievalProperties properties = properties();
        properties.validateConfiguration();
        return new RetrievalRequestValidator(properties);
    }

    private static RetrievalProperties properties() {
        return new RetrievalProperties();
    }

    private static void assertBudget(RetrievalBudget budget) {
        RetrievalRequest request = request();
        request.setBudget(budget);
        assertCode(request, RetrievalErrorCode.INVALID_REQUEST);
    }

    private static void assertCode(RetrievalRequest request, RetrievalErrorCode code) {
        RetrievalException exception = Assertions.assertThrows(RetrievalException.class,
            () -> validator().validate(request));
        Assertions.assertEquals(code, exception.getCode());
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
