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

package org.apache.geaflow.console.common.util.type;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.console.common.util.exception.GeaflowException;

public enum GeaflowTaskStatus {

    CREATED,

    WAITING,

    STARTING,

    FAILED,

    RUNNING,

    FINISHED,

    STOPPED,

    DELETED;

    private static final Map<GeaflowOperationType, Set<GeaflowTaskStatus>> allowedOperations = new HashMap<>();

    static {
        allowedOperations.put(GeaflowOperationType.START, EnumSet.of(CREATED, FAILED, STOPPED));
        allowedOperations.put(GeaflowOperationType.STOP, EnumSet.of(RUNNING, WAITING));
        allowedOperations.put(GeaflowOperationType.REFRESH, EnumSet.allOf(GeaflowTaskStatus.class));

        Set<GeaflowTaskStatus> unRunningStatus = EnumSet.of(CREATED, FAILED, STOPPED, FINISHED);
        allowedOperations.put(GeaflowOperationType.PUBLISH, unRunningStatus);
        allowedOperations.put(GeaflowOperationType.RESET, unRunningStatus);
        allowedOperations.put(GeaflowOperationType.DELETE, unRunningStatus);
        allowedOperations.put(GeaflowOperationType.FINISH, EnumSet.of(RUNNING, FINISHED));
    }


    public void checkOperation(GeaflowOperationType operationType) {
        Set<GeaflowTaskStatus> allowedStatuses = allowedOperations.get(operationType);
        if (allowedStatuses == null || !allowedStatuses.contains(this)) {
            throw new GeaflowException("Task {} status can't {}", this, operationType);
        }
    }
}
