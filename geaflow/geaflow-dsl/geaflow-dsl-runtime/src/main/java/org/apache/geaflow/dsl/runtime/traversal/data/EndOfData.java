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

package org.apache.geaflow.dsl.runtime.traversal.data;

import org.apache.geaflow.dsl.common.data.StepRecord;

public class EndOfData implements StepRecord {

    /**
     * The caller operator id. -1 means main dag execution.
     */
    private final long callOpId;

    /**
     * The sender operator id who send this data.
     */
    private final long senderId;

    /**
     * No data been processed between two eod cycles.
     */
    public boolean isGlobalEmptyCycle;

    private EndOfData(long callOpId, long senderId) {
        this.callOpId = callOpId;
        this.senderId = senderId;
    }

    private EndOfData(long senderId) {
        this(-1L, senderId);
    }

    public static EndOfData of(long senderId) {
        return new EndOfData(senderId);
    }

    public static EndOfData of(long callOpId, long senderId) {
        return new EndOfData(callOpId, senderId);
    }

    @Override
    public StepRecordType getType() {
        return StepRecordType.EOD;
    }

    public long getCallOpId() {
        return callOpId;
    }

    public long getSenderId() {
        return senderId;
    }

    @Override
    public String toString() {
        return "EndOfData{"
            + "callOpId=" + callOpId
            + ", senderId=" + senderId
            + '}';
    }
}
