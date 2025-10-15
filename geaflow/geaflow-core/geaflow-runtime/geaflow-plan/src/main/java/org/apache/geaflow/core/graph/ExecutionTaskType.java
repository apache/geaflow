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

package org.apache.geaflow.core.graph;

public enum ExecutionTaskType {
    /**
     * A head execution task is the start of cycle pipeline.
     * that receive event from scheduler to trigger a certain round of iteration.
     */
    head,

    /**
     * A middle execution task is the intermediate of cycle pipeline.
     */
    middle,

    /**
     * A tail execution task is the end of cycle pipeline.
     * that send event to scheduler to finish a certain round of iteration.
     */
    tail,

    /**
     * A singularity(start&end both) execution task is the start of cycle pipeline, also is the end of cycle pipeline at the same time.
     * Thus that receive event from scheduler to trigger a certain round of iteration,
     * and send event to scheduler to finish a certain round of iteration meanwhile.
     */
    singularity
}
