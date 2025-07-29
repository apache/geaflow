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

package org.apache.geaflow.api.graph.traversal;


import java.util.List;
import org.apache.geaflow.api.graph.base.algo.GraphExecAlgo;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.ITraversalResponse;

public interface PGraphTraversal<K, R> {

    /**
     * Start traversal all computing.
     */
    PWindowStream<ITraversalResponse<R>> start();

    /**
     * Start traversal computing with vid.
     */
    PWindowStream<ITraversalResponse<R>> start(K vId);

    /**
     * Start traversal computing with vid list.
     */
    PWindowStream<ITraversalResponse<R>> start(List<K> vId);

    /**
     * Start traversal computing with requests.
     */
    PWindowStream<ITraversalResponse<R>> start(PWindowStream<? extends ITraversalRequest<K>> requests);

    /**
     * Set the traversal parallelism.
     */
    PGraphTraversal<K, R> withParallelism(int parallelism);

    /**
     * Returns graph traversal type.
     */
    GraphExecAlgo getGraphTraversalType();

}
