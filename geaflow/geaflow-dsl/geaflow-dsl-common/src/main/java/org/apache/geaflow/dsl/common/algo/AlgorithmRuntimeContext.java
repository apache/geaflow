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

import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.state.pushdown.filter.IFilter;

/**
 * Interface defining methods for managing and interacting with the runtime context of a graph algorithm.
 *
 * @param <K> The type of vertex IDs.
 * @param <M> The type of messages that can be sent between vertices.
 */
public interface AlgorithmRuntimeContext<K, M> {

    /**
     * Loads all edges in the specified direction.
     *
     * @param direction The direction of the edges to be loaded.
     * @return A list of RowEdge objects representing the edges.
     */
    List<RowEdge> loadEdges(EdgeDirection direction);

    /**
     * Returns an iterator over all edges in the specified direction.
     *
     * @param direction The direction of the edges to iterate over.
     * @return An iterator over RowEdge objects representing the edges.
     */
    CloseableIterator<RowEdge> loadEdgesIterator(EdgeDirection direction);

    /**
     * Returns an iterator over edges filtered by the provided IFilter.
     *
     * @param filter The filter to apply when loading edges.
     * @return An iterator over RowEdge objects representing the filtered edges.
     */
    CloseableIterator<RowEdge> loadEdgesIterator(IFilter filter);

    /**
     * Loads static edges in the specified direction.
     *
     * @param direction The direction of the edges to be loaded.
     * @return A list of RowEdge objects representing the static edges.
     */
    List<RowEdge> loadStaticEdges(EdgeDirection direction);

    /**
     * Returns an iterator over static edges in the specified direction.
     *
     * @param direction The direction of the edges to iterate over.
     * @return An iterator over RowEdge objects representing the static edges.
     */
    CloseableIterator<RowEdge> loadStaticEdgesIterator(EdgeDirection direction);

    /**
     * Returns an iterator over static edges filtered by the provided IFilter.
     *
     * @param filter The filter to apply when loading edges.
     * @return An iterator over RowEdge objects representing the filtered static edges.
     */
    CloseableIterator<RowEdge> loadStaticEdgesIterator(IFilter filter);

    /**
     * Loads dynamic edges (changed during execution) in the specified direction.
     *
     * @param direction The direction of the edges to be loaded.
     * @return A list of RowEdge objects representing the dynamic edges.
     */
    List<RowEdge> loadDynamicEdges(EdgeDirection direction);

    /**
     * Returns an iterator over dynamic edges in the specified direction.
     *
     * @param direction The direction of the edges to iterate over.
     * @return An iterator over RowEdge objects representing the dynamic edges.
     */
    CloseableIterator<RowEdge> loadDynamicEdgesIterator(EdgeDirection direction);

    /**
     * Returns an iterator over dynamic edges filtered by the provided IFilter.
     *
     * @param filter The filter to apply when loading edges.
     * @return An iterator over RowEdge objects representing the filtered dynamic edges.
     */
    CloseableIterator<RowEdge> loadDynamicEdgesIterator(IFilter filter);

    /**
     * Sends a message to the specified vertex.
     *
     * @param vertexId The ID of the vertex to which the message should be sent.
     * @param message The message to send.
     */
    void sendMessage(K vertexId, M message);

    /**
     * Updates the current vertex's value with the new row data.
     *
     * @param value The new row data to set as the vertex value.
     */
    void updateVertexValue(Row value);

    /**
     * Takes a row of data, typically received from another source.
     *
     * @param value The row data to take.
     */
    void take(Row value);

    /**
     * Gets the unique identifier for the current iteration.
     *
     * @return The current iteration ID.
     */
    long getCurrentIterationId();

    /**
     * Retrieves the schema information for the graph.
     *
     * @return The GraphSchema object containing the graph structure details.
     */
    GraphSchema getGraphSchema();

    /**
     * Retrieves the configuration settings for the algorithm runtime context.
     *
     * @return The Configuration object containing the settings.
     */
    Configuration getConfig();
}