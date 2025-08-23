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

package org.apache.geaflow.runtime.core.scheduler.result;

public class ExecutionResult<R, E> implements IExecutionResult<R, E> {

    // Result of the execution.
    private R result;
    // Error object of the execution.
    private E error;
    // Check whether the execution is success.
    // If true, then the result should not empty, else the error should not empty.
    private boolean isSuccess;

    public ExecutionResult(R result, E error, boolean isSuccess) {
        this.result = result;
        this.error = error;
        this.isSuccess = isSuccess;
    }

    @Override
    public R getResult() {
        return result;
    }

    @Override
    public E getError() {
        return error;
    }

    @Override
    public boolean isSuccess() {
        return isSuccess;
    }

    public static <R, E> ExecutionResult<R, E> buildSuccessResult(R result) {
        return new ExecutionResult<>(result, null, true);
    }

    public static <R, E> ExecutionResult<R, E> buildFailedResult(E error) {
        return new ExecutionResult<>(null, error, false);
    }
}
