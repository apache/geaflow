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

package com.antgroup.geaflow.dsl.runtime.query.udf;

import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "test_edge_iterator", description = "built-in udga for WeakConnectedComponents")
public class TestEdgeIteratorUdf implements AlgorithmUserFunction<Object, Long> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestEdgeIteratorUdf.class);

    private AlgorithmRuntimeContext<Object, Long> context;

    private int iteration = 5;

    private int edgeLimit = 100;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Long> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 0) {
            iteration = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            edgeLimit = Integer.parseInt(String.valueOf(parameters[1]));
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Long> messages) {
        updatedValues.ifPresent(vertex::setValue);
        CloseableIterator<RowEdge> edgesIterator = context.loadStaticEdgesIterator(EdgeDirection.BOTH);
        if (context.getCurrentIterationId() < iteration) {
            int count = 0;
            while (edgesIterator.hasNext() && count < edgeLimit) {
                RowEdge next = edgesIterator.next();
                context.sendMessage(next.getTargetId(), context.getCurrentIterationId());
                count++;
            }
        }
        List<RowEdge> bothEdge = Lists.newArrayList(context.loadStaticEdgesIterator(EdgeDirection.BOTH));
        List<RowEdge> inEdge = Lists.newArrayList(context.loadStaticEdgesIterator(EdgeDirection.IN));
        List<RowEdge> outEdge = Lists.newArrayList(context.loadStaticEdgesIterator(EdgeDirection.OUT));
        Preconditions.checkState(bothEdge.size() == inEdge.size() + outEdge.size(), "Static edge not equal");
        List<RowEdge> bothEdgeList = context.loadStaticEdges(EdgeDirection.BOTH);
        Preconditions.checkState(bothEdgeList.size() == inEdge.size() + outEdge.size(), "Static edge not equal");

        bothEdge = Lists.newArrayList(context.loadDynamicEdgesIterator(EdgeDirection.BOTH));
        inEdge = Lists.newArrayList(context.loadDynamicEdgesIterator(EdgeDirection.IN));
        outEdge = Lists.newArrayList(context.loadDynamicEdgesIterator(EdgeDirection.OUT));
        Preconditions.checkState(bothEdge.size() == inEdge.size() + outEdge.size(), "Dynamic edge not equal");

        bothEdge = Lists.newArrayList(context.loadEdgesIterator(EdgeDirection.BOTH));
        inEdge = Lists.newArrayList(context.loadEdgesIterator(EdgeDirection.IN));
        outEdge = Lists.newArrayList(context.loadEdgesIterator(EdgeDirection.OUT));
        Preconditions.checkState(bothEdge.size() == inEdge.size() + outEdge.size(), "History edge not equal");


    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(graphVertex::setValue);
        long iteration = (long) graphVertex.getValue().getField(0, LongType.INSTANCE);
        context.take(ObjectRow.create(graphVertex.getId(), iteration));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("iteration", LongType.INSTANCE, false)
        );
    }
}
