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

package org.apache.geaflow.ai.retrieval.ingest;

import org.apache.geaflow.ai.retrieval.metadata.ImportState;
import org.apache.geaflow.ai.retrieval.metadata.MetadataException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** State transition coverage for one immutable import attempt. */
public class ImportStateMachineTest {

    @Test
    public void allowsOnlyForwardBuildTransitionsAndFailure() {
        Assertions.assertTrue(ImportStateMachine.canTransition(ImportState.IMPORTING,
            ImportState.INDEXING));
        Assertions.assertTrue(ImportStateMachine.canTransition(ImportState.IMPORTING,
            ImportState.FAILED));
        Assertions.assertTrue(ImportStateMachine.canTransition(ImportState.INDEXING,
            ImportState.READY));
        Assertions.assertTrue(ImportStateMachine.canTransition(ImportState.INDEXING,
            ImportState.FAILED));
        Assertions.assertEquals(ImportState.INDEXING, ImportStateMachine.transition(
            ImportState.IMPORTING, ImportState.INDEXING));
    }

    @Test
    public void rejectsTerminalAndBackwardTransitionsWithTypedError() {
        ImportState[] states = ImportState.values();
        for (ImportState current : states) {
            for (ImportState target : states) {
                if (!ImportStateMachine.canTransition(current, target)) {
                    MetadataException exception = Assertions.assertThrows(MetadataException.class,
                        () -> ImportStateMachine.validate(current, target));
                    Assertions.assertEquals(MetadataException.Code.INVALID_TRANSITION,
                        exception.getCode());
                }
            }
        }
    }
}
