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

package org.apache.geaflow.ai.temporal.semantics;

/** Stable validation failures for normalized temporal events. */
public enum TemporalErrorCode {
    REJECT_INPUT,
    REJECT_SCHEMA_VERSION,
    REJECT_REFERENCE_INTEGRITY,
    REJECT_TARGET_REQUIRED,
    REJECT_TARGET_NOT_FOUND,
    REJECT_TARGET_INACTIVE,
    REJECT_TARGET_NO_OVERLAP,
    REJECT_EVENT_ID_REUSE
}
