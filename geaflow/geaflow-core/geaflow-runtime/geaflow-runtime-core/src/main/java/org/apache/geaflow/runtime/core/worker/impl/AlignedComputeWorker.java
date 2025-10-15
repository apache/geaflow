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

package org.apache.geaflow.runtime.core.worker.impl;

import org.apache.geaflow.cluster.worker.WorkerType;
import org.apache.geaflow.runtime.core.worker.AbstractAlignedWorker;

public class AlignedComputeWorker<T, R> extends AbstractAlignedWorker<T, R> {

    public AlignedComputeWorker() {
        super();
    }

    @Override
    public WorkerType getWorkerType() {
        return WorkerType.aligned_compute;
    }
}
