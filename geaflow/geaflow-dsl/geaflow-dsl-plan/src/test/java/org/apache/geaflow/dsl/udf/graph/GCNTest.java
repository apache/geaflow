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

package org.apache.geaflow.dsl.udf.graph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.dsl.common.algo.AlgorithmModelRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectVertex;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNExpandMessage;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNFragmentMessage;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNState;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNEdgeRecord;
import org.apache.geaflow.dsl.udf.graph.gcn.MergedNeighborhoodCollector;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.testng.annotations.Test;

public class GCNTest {

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "GCN requires model runtime context")
    public void testInitRejectsNonModelContext() {
        new GCN().init(new FakeRuntimeContext(new Configuration(), simpleGraphSchema()), new Object[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "geaflow\\.dsl\\.gcn\\.hops must be >= 1")
    public void testInitRejectsInvalidHops() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.hops", "0");
        new GCN().init(new FakeModelRuntimeContext(config, simpleGraphSchema()), new Object[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "geaflow\\.dsl\\.gcn\\.fanout must be -1 or > 0")
    public void testInitRejectsInvalidFanout() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.fanout", "0");
        new GCN().init(new FakeModelRuntimeContext(config, simpleGraphSchema()), new Object[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "geaflow\\.dsl\\.gcn\\.edge\\.direction must be one of IN, OUT, BOTH")
    public void testInitRejectsInvalidDirection() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.edge.direction", "sideways");
        new GCN().init(new FakeModelRuntimeContext(config, simpleGraphSchema()), new Object[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "geaflow\\.dsl\\.gcn\\.vertex\\.feature\\.fields must be configured")
    public void testInitRejectsMissingFeatureFields() {
        new GCN().init(new FakeModelRuntimeContext(new Configuration(), simpleGraphSchema()),
            new Object[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "GCN feature field must be numeric scalar: name")
    public void testInitRejectsNonNumericFeatureField() {
        Configuration config = new Configuration();
        config.put("geaflow.dsl.gcn.vertex.feature.fields", "name");
        new GCN().init(new FakeModelRuntimeContext(config, simpleGraphSchema()), new Object[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "GCN feature fields are inconsistent across vertex value schemas")
    public void testInitRejectsInconsistentFeatureSchema() {
        Configuration config = baseConfig();
        GraphSchema schema = graphSchema(
            "person", vertexType(new TableField("name", StringType.INSTANCE),
                new TableField("age", IntegerType.INSTANCE)),
            "software", vertexType(new TableField("name", StringType.INSTANCE),
                new TableField("age", LongType.INSTANCE)),
            "knows", edgeType(new TableField("weight", DoubleType.INSTANCE))
        );
        new GCN().init(new FakeModelRuntimeContext(config, schema), new Object[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "GCN edge weight field 'missing' missing from edge label 'knows'")
    public void testInitRejectsMissingEdgeWeightField() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.edge.weight.field", "missing");
        new GCN().init(new FakeModelRuntimeContext(config, simpleGraphSchema()), new Object[0]);
    }

    @Test
    public void testMergedNeighborhoodUsesDynamicValue() {
        MergedNeighborhoodCollector collector = new MergedNeighborhoodCollector();
        Row historyValue = ObjectRow.create("history", 18);
        Row dynamicValue = ObjectRow.create("dynamic", 20);
        ObjectVertex historyVertex = new ObjectVertex(1L);
        historyVertex.setLabel("person");
        historyVertex.setValue(historyValue);
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(baseConfig(), simpleGraphSchema());
        context.dynamicVertexValue = dynamicValue;

        Row mergedValue = collector.resolveMergedVertexValue(1L, historyVertex, context);

        assertSame(mergedValue, dynamicValue);
    }

    @Test
    public void testMergedNeighborhoodSamplingDoesNotDependOnInputOrder() {
        MergedNeighborhoodCollector collector = new MergedNeighborhoodCollector();
        RowEdge staticEdge1 = edge(1L, 2L, "knows", EdgeDirection.OUT, 1.0D);
        RowEdge staticEdge2 = edge(1L, 3L, "knows", EdgeDirection.OUT, 2.0D);
        RowEdge dynamicEdge = edge(1L, 4L, "knows", EdgeDirection.OUT, 3.0D);
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(baseConfig(), simpleGraphSchema());
        context.dynamicEdges = Collections.singletonList(dynamicEdge);

        List<RowEdge> sampledEdgesA = collector.collectMergedEdges(1L,
            Arrays.asList(staticEdge1, staticEdge2), context, EdgeDirection.OUT, 2);
        List<RowEdge> sampledEdgesB = collector.collectMergedEdges(1L,
            Arrays.asList(staticEdge2, staticEdge1), context, EdgeDirection.OUT, 2);

        assertEquals(edgeSignatures(sampledEdgesA), edgeSignatures(sampledEdgesB));
    }

    @Test
    public void testProcessRebuildsStateOnFirstIteration() {
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(baseConfig(), simpleGraphSchema());
        context.iterationId = 1L;
        context.staticEdges = Collections.singletonList(edge(1L, 2L, "knows", EdgeDirection.OUT, 1.0D));

        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        algorithm.process(vertex(1L, "person", "alice", 18),
            Optional.<Row>empty(), Collections.<Object>emptyList().iterator());

        GCNState state = (GCNState) context.updatedValue.getField(0, ObjectType.INSTANCE);
        assertEquals(state.getMinDepthByRoot().get(1L), Integer.valueOf(0));
        assertTrue(state.getAccumulator().getNodeFeatures().containsKey(1L));
        assertEquals(context.sentMessages.size(), 1);
        assertEquals(context.sentMessages.get(0).vertexId, 2L);
        GCNExpandMessage message = (GCNExpandMessage) context.sentMessages.get(0).message;
        assertEquals(message.getRootId(), 1L);
        assertEquals(message.getDepth(), 1);
    }

    @Test
    public void testProcessDeduplicatesRootExpansionByDepth() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.hops", "3");
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(config, simpleGraphSchema());
        context.iterationId = 3L;
        context.staticEdges = Arrays.asList(
            edge(4L, 2L, "knows", EdgeDirection.OUT, 1.0D),
            edge(4L, 5L, "knows", EdgeDirection.OUT, 1.0D));

        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        GCNState state = new GCNState();
        GCNExpandMessage first = new GCNExpandMessage(1L, 2);
        GCNExpandMessage second = new GCNExpandMessage(1L, 3);
        algorithm.process(vertex(4L, "person", "diana", 40), Optional.of(ObjectRow.create(state)),
            Arrays.<Object>asList(first, second).iterator());

        int fragmentToRoot = 0;
        int expandTo2 = 0;
        int expandTo5 = 0;
        for (SentMessage sentMessage : context.sentMessages) {
            if (sentMessage.vertexId.equals(1L)) {
                fragmentToRoot++;
            } else if (sentMessage.vertexId.equals(2L)) {
                expandTo2++;
            } else if (sentMessage.vertexId.equals(5L)) {
                expandTo5++;
            }
        }
        assertEquals(fragmentToRoot, 1);
        assertEquals(expandTo2, 1);
        assertEquals(expandTo5, 1);
        GCNState newState = (GCNState) context.updatedValue.getField(0, ObjectType.INSTANCE);
        assertEquals(newState.getMinDepthByRoot().get(1L), Integer.valueOf(2));
    }

    @Test
    public void testProcessWithInDirectionOnlyUsesIncomingNeighbors() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.edge.direction", "IN");
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(config, simpleGraphSchema());
        context.iterationId = 1L;
        context.staticEdges = Arrays.asList(
            edge(2L, 1L, "knows", EdgeDirection.IN, 1.0D),
            edge(1L, 3L, "knows", EdgeDirection.OUT, 1.0D));

        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        algorithm.process(vertex(1L, "person", "alice", 18),
            Optional.<Row>empty(), Collections.<Object>emptyList().iterator());

        assertEquals(context.sentMessages.size(), 1);
        assertEquals(context.sentMessages.get(0).vertexId, 2L);
        GCNState state = (GCNState) context.updatedValue.getField(0, ObjectType.INSTANCE);
        assertEquals(state.getAccumulator().getEdgeRecords().size(), 1);
        GCNEdgeRecord edgeRecord = state.getAccumulator().getEdgeRecords().get(0);
        assertEquals(edgeRecord.getSrcId(), 2L);
        assertEquals(edgeRecord.getTargetId(), 1L);
    }

    @Test
    public void testProcessWithBothDirectionUsesIncomingAndOutgoingNeighbors() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.edge.direction", "BOTH");
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(config, simpleGraphSchema());
        context.iterationId = 1L;
        context.staticEdges = Arrays.asList(
            edge(2L, 1L, "knows", EdgeDirection.IN, 1.0D),
            edge(1L, 3L, "knows", EdgeDirection.OUT, 1.0D));

        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        algorithm.process(vertex(1L, "person", "alice", 18),
            Optional.<Row>empty(), Collections.<Object>emptyList().iterator());

        assertEquals(context.sentMessages.size(), 2);
        assertEquals(sentTargets(context.sentMessages), Arrays.asList(2L, 3L));
    }

    @Test
    public void testFinishBatchWithFanoutOneKeepsDeterministicSubset() {
        MergedNeighborhoodCollector collector = new MergedNeighborhoodCollector();
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(baseConfig(), simpleGraphSchema());
        List<RowEdge> edgesA = Arrays.asList(
            edge(1L, 2L, "knows", EdgeDirection.OUT, 1.0D),
            edge(1L, 3L, "knows", EdgeDirection.OUT, 1.0D),
            edge(1L, 4L, "knows", EdgeDirection.OUT, 1.0D));
        List<RowEdge> edgesB = Arrays.asList(
            edge(1L, 4L, "knows", EdgeDirection.OUT, 1.0D),
            edge(1L, 2L, "knows", EdgeDirection.OUT, 1.0D),
            edge(1L, 3L, "knows", EdgeDirection.OUT, 1.0D));

        List<RowEdge> sampledA = collector.collectMergedEdges(1L, edgesA, context,
            EdgeDirection.OUT, 1);
        List<RowEdge> sampledB = collector.collectMergedEdges(1L, edgesB, context,
            EdgeDirection.OUT, 1);

        assertEquals(sampledA.size(), 1);
        assertEquals(edgeSignatures(sampledA), edgeSignatures(sampledB));
    }

    @Test
    public void testExtractFeaturesUsesZeroForNullFields() {
        Configuration config = new Configuration();
        config.put("geaflow.dsl.gcn.vertex.feature.fields", "age,height");
        GraphSchema schema = graphSchema(
            "person", vertexType(
                new TableField("name", StringType.INSTANCE),
                new TableField("age", IntegerType.INSTANCE),
                new TableField("height", LongType.INSTANCE)),
            "knows", edgeType(new TableField("weight", DoubleType.INSTANCE))
        );
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(config, schema);
        context.iterationId = 1L;

        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        algorithm.process(vertexWithValue(1L, "person", ObjectRow.create("alice", null, 170L)),
            Optional.<Row>empty(), Collections.<Object>emptyList().iterator());

        GCNState state = (GCNState) context.updatedValue.getField(0, ObjectType.INSTANCE);
        assertEquals(state.getAccumulator().getNodeFeatures().get(1L), Arrays.asList(0.0D, 170.0D));
    }

    @Test
    public void testExtractEdgeWeightUsesZeroWhenWeightFieldNull() {
        Configuration config = baseConfig();
        config.put("geaflow.dsl.gcn.edge.weight.field", "weight");
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(config, simpleGraphSchema());
        context.iterationId = 1L;
        context.staticEdges = Collections.singletonList(
            edgeWithValue(1L, 2L, "knows", EdgeDirection.OUT, ObjectRow.create((Object) null)));

        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        algorithm.process(vertex(1L, "person", "alice", 18),
            Optional.<Row>empty(), Collections.<Object>emptyList().iterator());

        GCNState state = (GCNState) context.updatedValue.getField(0, ObjectType.INSTANCE);
        assertEquals(state.getAccumulator().getEdgeRecords().get(0).getWeight(), 0.0D);
    }

    @Test
    public void testFinishBatchRejectsInferResultSizeMismatch() {
        Configuration config = baseConfig();
        config.put("geaflow.infer.env.enable", "true");
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(config, simpleGraphSchema()) {
            @Override
            public List<Object> inferBatch(List<Map<String, Object>> payloads) {
                batchInferPayloads.add(new ArrayList<Map<String, Object>>(payloads));
                return Collections.emptyList();
            }
        };
        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        assertThrows(IllegalArgumentException.class, () -> algorithm.finishBatch(
            Collections.<RowVertex>singletonList(vertex(1L, "person", "alice", 18)),
            Collections.<Optional<Row>>singletonList(Optional.of(ObjectRow.create(
                stateWithEdge(1L, 2L, 1.0D, 18.0D, 20.0D))))));
    }

    @Test
    public void testFinishBatchUsesConfiguredBatchSizeAndPreservesDirectedWeights() {
        Configuration config = baseConfig();
        config.put("geaflow.infer.env.enable", "true");
        config.put("geaflow.dsl.gcn.batch.size", "2");
        config.put("geaflow.dsl.gcn.edge.weight.field", "weight");
        FakeModelRuntimeContext context = new FakeModelRuntimeContext(config, simpleGraphSchema());
        GCN algorithm = new GCN();
        algorithm.init(context, new Object[0]);

        GCNState first = stateWithEdge(1L, 2L, 2.0D, 18.0D, 20.0D);
        GCNState second = stateWithEdge(2L, 3L, 4.0D, 20.0D, 22.0D);
        GCNState third = stateWithEdge(3L, 4L, 6.0D, 22.0D, 24.0D);

        algorithm.finishBatch(Arrays.asList(
                vertex(1L, "person", "alice", 18),
                vertex(2L, "person", "bob", 20),
                vertex(3L, "person", "cathy", 22)),
            Arrays.asList(
                Optional.of(ObjectRow.create(first)),
                Optional.of(ObjectRow.create(second)),
                Optional.of(ObjectRow.create(third))));

        assertEquals(context.batchInferPayloads.size(), 2);
        assertEquals(context.batchInferPayloads.get(0).size(), 2);
        assertEquals(context.batchInferPayloads.get(1).size(), 1);
        Map<String, Object> firstPayload = context.batchInferPayloads.get(0).get(0);
        List<?> edgeIndex = (List<?>) firstPayload.get("edge_index");
        assertEquals(edgeIndex.get(0), Arrays.asList(0, 0, 1));
        assertEquals(edgeIndex.get(1), Arrays.asList(1, 0, 1));
        List<?> edgeWeight = (List<?>) firstPayload.get("edge_weight");
        assertEquals(((Double) edgeWeight.get(0)).doubleValue(), 2.0D / Math.sqrt(3.0D), 1e-9);
        assertEquals(context.takenRows.size(), 3);
        assertEquals(context.takenRows.get(0).getField(0, ObjectType.INSTANCE), 1L);
        assertEquals(context.takenRows.get(2).getField(0, ObjectType.INSTANCE), 3L);
    }

    private static GCNState stateWithEdge(long srcId, long targetId, double weight,
                                          double srcFeature, double targetFeature) {
        GCNState state = new GCNState();
        state.initAccumulator(srcId);
        state.getAccumulator().addFragment(new GCNFragmentMessage(srcId, srcId,
            Collections.singletonList(srcFeature),
            Collections.singletonList(new GCNEdgeRecord(srcId, targetId, "knows",
                EdgeDirection.OUT, weight)), false));
        state.getAccumulator().addFragment(new GCNFragmentMessage(srcId, targetId,
            Collections.singletonList(targetFeature), Collections.emptyList(), false));
        return state;
    }

    private static Configuration baseConfig() {
        Configuration config = new Configuration();
        config.put("geaflow.dsl.gcn.vertex.feature.fields", "age");
        return config;
    }

    private static List<String> edgeSignatures(List<RowEdge> edges) {
        List<String> signatures = new ArrayList<>();
        for (RowEdge edge : edges) {
            signatures.add(edge.getSrcId() + "->" + edge.getTargetId() + "#" + edge.getDirect()
                + "@" + edge.getValue().getField(0, ObjectType.INSTANCE));
        }
        return signatures;
    }

    private static List<Long> sentTargets(List<SentMessage> messages) {
        List<Long> targets = new ArrayList<Long>(messages.size());
        for (SentMessage message : messages) {
            targets.add((Long) message.vertexId);
        }
        Collections.sort(targets);
        return targets;
    }

    private static GraphSchema simpleGraphSchema() {
        return graphSchema(
            "person", vertexType(
                new TableField("name", StringType.INSTANCE),
                new TableField("age", IntegerType.INSTANCE)),
            "knows", edgeType(new TableField("weight", DoubleType.INSTANCE))
        );
    }

    private static GraphSchema graphSchema(String vertexLabel, VertexType vertexType, String edgeLabel,
                                           EdgeType edgeType) {
        return new GraphSchema("g", Arrays.asList(
            new TableField(vertexLabel, vertexType),
            new TableField(edgeLabel, edgeType)));
    }

    private static GraphSchema graphSchema(String vertexLabel1, VertexType vertexType1,
                                           String vertexLabel2, VertexType vertexType2,
                                           String edgeLabel, EdgeType edgeType) {
        return new GraphSchema("g", Arrays.asList(
            new TableField(vertexLabel1, vertexType1),
            new TableField(vertexLabel2, vertexType2),
            new TableField(edgeLabel, edgeType)));
    }

    private static VertexType vertexType(TableField... valueFields) {
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField(VertexType.DEFAULT_ID_FIELD_NAME, LongType.INSTANCE, false));
        fields.add(new TableField(GraphSchema.LABEL_FIELD_NAME, StringType.INSTANCE, false));
        fields.addAll(Arrays.asList(valueFields));
        return new VertexType(fields);
    }

    private static EdgeType edgeType(TableField... valueFields) {
        List<TableField> fields = new ArrayList<>();
        fields.add(new TableField(EdgeType.DEFAULT_SRC_ID_NAME, LongType.INSTANCE, false));
        fields.add(new TableField(EdgeType.DEFAULT_TARGET_ID_NAME, LongType.INSTANCE, false));
        fields.add(new TableField(GraphSchema.LABEL_FIELD_NAME, StringType.INSTANCE, false));
        fields.addAll(Arrays.asList(valueFields));
        return new EdgeType(fields, false);
    }

    private static RowEdge edge(long srcId, long targetId, String label, EdgeDirection direction,
                                double weight) {
        ObjectEdge edge = new ObjectEdge(srcId, targetId, ObjectRow.create(weight));
        edge.setLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    private static RowEdge edgeWithValue(long srcId, long targetId, String label,
                                         EdgeDirection direction, Row value) {
        ObjectEdge edge = new ObjectEdge(srcId, targetId, value);
        edge.setLabel(label);
        edge.setDirect(direction);
        return edge;
    }

    private static ObjectVertex vertex(long id, String label, String name, int age) {
        return vertexWithValue(id, label, ObjectRow.create(name, age));
    }

    private static ObjectVertex vertexWithValue(long id, String label, Row value) {
        ObjectVertex vertex = new ObjectVertex(id);
        vertex.setLabel(label);
        vertex.setValue(value);
        return vertex;
    }

    private static class FakeRuntimeContext implements AlgorithmRuntimeContext<Object, Object> {

        private final Configuration config;
        private final GraphSchema graphSchema;
        long iterationId;
        List<RowEdge> staticEdges = Collections.emptyList();
        Row updatedValue;
        final List<SentMessage> sentMessages = new ArrayList<>();
        final List<Row> takenRows = new ArrayList<>();

        FakeRuntimeContext(Configuration config, GraphSchema graphSchema) {
            this.config = config;
            this.graphSchema = graphSchema;
        }

        @Override
        public List<RowEdge> loadEdges(EdgeDirection direction) {
            return Collections.emptyList();
        }

        @Override
        public CloseableIterator<RowEdge> loadEdgesIterator(EdgeDirection direction) {
            return null;
        }

        @Override
        public CloseableIterator<RowEdge> loadEdgesIterator(IFilter filter) {
            return null;
        }

        @Override
        public List<RowEdge> loadStaticEdges(EdgeDirection direction) {
            return filterEdges(staticEdges, direction);
        }

        @Override
        public CloseableIterator<RowEdge> loadStaticEdgesIterator(EdgeDirection direction) {
            return null;
        }

        @Override
        public CloseableIterator<RowEdge> loadStaticEdgesIterator(IFilter filter) {
            return null;
        }

        @Override
        public List<RowEdge> loadDynamicEdges(EdgeDirection direction) {
            return Collections.emptyList();
        }

        @Override
        public CloseableIterator<RowEdge> loadDynamicEdgesIterator(EdgeDirection direction) {
            return null;
        }

        @Override
        public CloseableIterator<RowEdge> loadDynamicEdgesIterator(IFilter filter) {
            return null;
        }

        @Override
        public void sendMessage(Object vertexId, Object message) {
            sentMessages.add(new SentMessage(vertexId, message));
        }

        @Override
        public void updateVertexValue(Row value) {
            updatedValue = value;
        }

        @Override
        public void take(Row value) {
            takenRows.add(value);
        }

        @Override
        public long getCurrentIterationId() {
            return iterationId;
        }

        @Override
        public GraphSchema getGraphSchema() {
            return graphSchema;
        }

        @Override
        public Configuration getConfig() {
            return config;
        }

        @Override
        public void voteToTerminate(String terminationReason, Object voteValue) {
        }

        protected List<RowEdge> filterEdges(List<RowEdge> edges, EdgeDirection direction) {
            if (direction == EdgeDirection.BOTH) {
                return edges;
            }
            List<RowEdge> filtered = new ArrayList<RowEdge>();
            for (RowEdge edge : edges) {
                if (edge.getDirect() == direction) {
                    filtered.add(edge);
                }
            }
            return filtered;
        }
    }

    private static class SentMessage {

        private final Object vertexId;
        private final Object message;

        private SentMessage(Object vertexId, Object message) {
            this.vertexId = vertexId;
            this.message = message;
        }
    }

    private static class FakeModelRuntimeContext extends FakeRuntimeContext
        implements AlgorithmModelRuntimeContext<Object, Object> {

        private Row dynamicVertexValue;
        private List<RowEdge> dynamicEdges = Collections.emptyList();
        protected final List<List<Map<String, Object>>> batchInferPayloads = new ArrayList<>();

        FakeModelRuntimeContext(Configuration config, GraphSchema graphSchema) {
            super(config, graphSchema);
        }

        @Override
        public Object infer(Map<String, Object> payload) {
            return inferBatch(Collections.singletonList(payload)).get(0);
        }

        @Override
        public List<Object> inferBatch(List<Map<String, Object>> payloads) {
            batchInferPayloads.add(new ArrayList<>(payloads));
            List<Object> results = new ArrayList<>(payloads.size());
            for (Map<String, Object> payload : payloads) {
                results.add(Collections.singletonMap("embedding",
                    Collections.singletonList(payload.get("center_node_id"))));
            }
            return results;
        }

        @Override
        public Row loadDynamicVertexValue(Object vertexId) {
            return dynamicVertexValue;
        }

        @Override
        public List<RowEdge> loadDynamicEdges(Object vertexId, EdgeDirection direction) {
            return filterEdges(dynamicEdges, direction);
        }
    }
}
