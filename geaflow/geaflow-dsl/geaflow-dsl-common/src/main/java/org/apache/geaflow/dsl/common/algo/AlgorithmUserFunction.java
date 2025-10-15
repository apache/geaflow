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

package org.apache.geaflow.dsl.common.algo;


import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;

/**
 * Interface for the User Defined Graph Algorithm.
 *
 * @param <K> The id type for vertex.
 * @param <M> The message type for message send between vertices.
 */
public interface AlgorithmUserFunction<K, M> extends Serializable {

    /**
     * Init method for the function.
     *
     * @param context The runtime context.
     * @param params  The parameters for the function.
     */
    void init(AlgorithmRuntimeContext<K, M> context, Object[] params);

    /**
     * Processing method for each vertex and the messages it received.
     */
    void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<M> messages);

    /**
     * Finish method called by each vertex upon algorithm convergence.
     */
    void finish(RowVertex graphVertex, Optional<Row> updatedValues);

    /**
     * Finish method called after all vertices is processed.
     */
    default void finish() {
    }

    /**
     * Finish Iteration method called after each iteration finished.
     */
    default void finishIteration(long iterationId) {
    }

    /**
     * Returns the output type for the function.
     */
    StructType getOutputType(GraphSchema graphSchema);
}
