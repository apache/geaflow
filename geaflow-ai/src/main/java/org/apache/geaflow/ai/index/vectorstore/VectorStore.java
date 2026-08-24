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

package org.apache.geaflow.ai.index.vectorstore;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.geaflow.ai.index.vector.IVector;

public interface VectorStore {

    /**
     * Get the metadata of this vector store.
     *
     * @return The vector store metadata.
     */
    VectorStoreMetadata getMetadata();

    /**
     * Initialize the vector store.
     * Implementations should validate the expected metadata against the stored metadata.
     *
     * @param expectedMetadata The expected metadata for validation.
     */
    void init(VectorStoreMetadata expectedMetadata);

    /**
     * Add a vector to the store with a given ID.
     *
     * @param id     The identifier for the vector.
     * @param vector The vector to store.
     */
    void add(String id, IVector vector);

    /**
     * Delete a vector by ID.
     *
     * @param id The identifier for the vector.
     */
    void delete(String id);

    /**
     * Search the top-K closest vectors to the query vector.
     *
     * @param queryVector The query vector.
     * @param topK        The number of results to return.
     * @return A list of pairs containing the vector ID and distance score.
     */
    List<Pair<String, Double>> search(IVector queryVector, int topK);

    /**
     * Close the vector store and release any resources.
     */
    void close();
}
