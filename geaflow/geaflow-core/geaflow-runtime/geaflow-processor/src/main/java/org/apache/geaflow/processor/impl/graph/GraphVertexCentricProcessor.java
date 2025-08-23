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

package org.apache.geaflow.processor.impl.graph;

import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.message.IGraphMessage;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.record.BatchRecord;
import org.apache.geaflow.model.record.RecordArgs;
import org.apache.geaflow.model.record.RecordArgs.GraphRecordNames;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphAggregateOp;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphTraversalOp;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import org.apache.geaflow.operator.impl.iterator.IteratorOperator;
import org.apache.geaflow.processor.impl.AbstractWindowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphVertexCentricProcessor<U, OP extends IGraphVertexCentricOp & IteratorOperator>
    extends AbstractWindowProcessor<BatchRecord<U>, Void, OP> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphVertexCentricProcessor.class);

    private long iterations;

    public GraphVertexCentricProcessor(OP graphVertexCentricOp) {
        super(graphVertexCentricOp);
    }

    @Override
    public void open(List<ICollector> collectors, RuntimeContext runtimeContext) {
        super.open(collectors, runtimeContext);
    }

    @Override
    public void init(long batchId) {
        this.iterations = batchId;
    }

    @Override
    public Void process(BatchRecord batchRecord) {
        if (batchRecord != null) {
            this.operator.initIteration(iterations);
            if (this.operator.getMaxIterationCount() >= iterations) {
                RecordArgs recordArgs = batchRecord.getRecordArgs();
                GraphRecordNames graphRecordName = GraphRecordNames.valueOf(recordArgs.getName());
                if (graphRecordName == GraphRecordNames.Vertex) {
                    final Iterator<IVertex> vertexIterator = batchRecord.getMessageIterator();
                    while (vertexIterator.hasNext()) {
                        IVertex vertex = vertexIterator.next();
                        this.operator.addVertex(vertex);
                    }
                } else if (graphRecordName == GraphRecordNames.Edge) {
                    final Iterator<IEdge> edgeIterator = batchRecord.getMessageIterator();
                    while (edgeIterator.hasNext()) {
                        this.operator.addEdge(edgeIterator.next());
                    }
                } else if (graphRecordName == GraphRecordNames.Message) {
                    final Iterator<IGraphMessage> graphMessageIterator =
                        batchRecord.getMessageIterator();
                    while (graphMessageIterator.hasNext()) {
                        this.operator.processMessage(graphMessageIterator.next());
                    }
                } else if (graphRecordName == GraphRecordNames.Request) {
                    final Iterator<ITraversalRequest> requestIterator =
                        batchRecord.getMessageIterator();
                    while (requestIterator.hasNext()) {
                        ((IGraphTraversalOp) this.operator).addRequest(requestIterator.next());
                    }
                } else if (graphRecordName == GraphRecordNames.Aggregate) {
                    final Iterator<ITraversalRequest> requestIterator =
                        batchRecord.getMessageIterator();
                    while (requestIterator.hasNext()) {
                        ((IGraphAggregateOp) this.operator).processAggregateResult(requestIterator.next());
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void finish(long batchId) {
        if (batchId > 0) {
            this.operator.finishIteration(iterations);
        } else {
            this.operator.finish();
        }
    }

}
