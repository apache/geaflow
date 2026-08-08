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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.geaflow.common.config.ConfigHelper;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.ByteType;
import org.apache.geaflow.common.type.primitive.DecimalType;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.FloatType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.common.type.primitive.ShortType;
import org.apache.geaflow.dsl.common.algo.AlgorithmModelRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.BatchAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.ArrayType;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNEdgeRecord;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNExpandMessage;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNFragmentMessage;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNPayloadAssembler;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNResultDecoder;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNState;
import org.apache.geaflow.dsl.udf.graph.gcn.MergedNeighborhoodCollector;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "gcn", description = "built-in udga for GCN node inference")
public class GCN implements AlgorithmUserFunction<Object, Object>,
    BatchAlgorithmUserFunction<Object, Object>, IncrementalAlgorithmUserFunction {

    private AlgorithmRuntimeContext<Object, Object> context;
    private AlgorithmModelRuntimeContext<Object, Object> modelContext;
    private final MergedNeighborhoodCollector neighborhoodCollector = new MergedNeighborhoodCollector();
    private final GCNPayloadAssembler payloadAssembler = new GCNPayloadAssembler();
    private final GCNResultDecoder resultDecoder = new GCNResultDecoder();

    private int hops;
    private int fanout;
    private int batchSize;
    private EdgeDirection edgeDirection;
    private Map<String, int[]> featureIndexesByLabel;
    private Map<String, Integer> edgeWeightIndexesByLabel;
    private boolean inferEnabled;
    private boolean edgeWeightEnabled;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        if (parameters.length > 0) {
            throw new IllegalArgumentException("CALL gcn() does not accept arguments");
        }
        this.context = context;
        if (!(context instanceof AlgorithmModelRuntimeContext)) {
            throw new IllegalArgumentException("GCN requires model runtime context");
        }
        this.modelContext = (AlgorithmModelRuntimeContext<Object, Object>) context;
        this.inferEnabled = ConfigHelper.getBooleanOrDefault(context.getConfig().getConfigMap(),
            FrameworkConfigKeys.INFER_ENV_ENABLE.getKey(), false);
        this.hops = ConfigHelper.getIntegerOrDefault(context.getConfig().getConfigMap(),
            DSLConfigKeys.GEAFLOW_DSL_GCN_HOPS.getKey(),
            (Integer) DSLConfigKeys.GEAFLOW_DSL_GCN_HOPS.getDefaultValue());
        if (hops < 1) {
            throw new IllegalArgumentException("geaflow.dsl.gcn.hops must be >= 1");
        }
        this.fanout = ConfigHelper.getIntegerOrDefault(context.getConfig().getConfigMap(),
            DSLConfigKeys.GEAFLOW_DSL_GCN_FANOUT.getKey(),
            (Integer) DSLConfigKeys.GEAFLOW_DSL_GCN_FANOUT.getDefaultValue());
        if (fanout == 0 || fanout < -1) {
            throw new IllegalArgumentException("geaflow.dsl.gcn.fanout must be -1 or > 0");
        }
        this.batchSize = ConfigHelper.getIntegerOrDefault(context.getConfig().getConfigMap(),
            DSLConfigKeys.GEAFLOW_DSL_GCN_BATCH_SIZE.getKey(),
            (Integer) DSLConfigKeys.GEAFLOW_DSL_GCN_BATCH_SIZE.getDefaultValue());
        if (batchSize < 1) {
            throw new IllegalArgumentException("geaflow.dsl.gcn.batch.size must be >= 1");
        }
        String direction = ConfigHelper.getStringOrDefault(context.getConfig().getConfigMap(),
            DSLConfigKeys.GEAFLOW_DSL_GCN_EDGE_DIRECTION.getKey(),
            String.valueOf(DSLConfigKeys.GEAFLOW_DSL_GCN_EDGE_DIRECTION.getDefaultValue()));
        this.edgeDirection = parseEdgeDirection(direction);

        String featureFieldConfig = context.getConfig()
            .getString(DSLConfigKeys.GEAFLOW_DSL_GCN_VERTEX_FEATURE_FIELDS.getKey());
        if (featureFieldConfig == null || featureFieldConfig.trim().isEmpty()) {
            throw new IllegalArgumentException("geaflow.dsl.gcn.vertex.feature.fields must be configured");
        }
        List<String> featureFields = parseFields(featureFieldConfig, "GCN feature fields");
        this.featureIndexesByLabel = resolveVertexFeatureIndexes(context.getGraphSchema(), featureFields);

        String edgeWeightField = context.getConfig()
            .getString(DSLConfigKeys.GEAFLOW_DSL_GCN_EDGE_WEIGHT_FIELD.getKey());
        this.edgeWeightEnabled = edgeWeightField != null && !edgeWeightField.trim().isEmpty();
        this.edgeWeightIndexesByLabel = resolveEdgeWeightIndexes(context.getGraphSchema(),
            edgeWeightEnabled ? edgeWeightField.trim() : null);
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        if (vertex == null) {
            return;
        }
        if (context.getCurrentIterationId() == 1L) {
            initializeWindowState(vertex);
            return;
        }

        Object vertexId = vertex.getId();
        GCNState state = loadState(updatedValues);
        List<GCNFragmentMessage> fragmentMessages = new ArrayList<>();
        Map<Object, GCNExpandMessage> expandMessages = new LinkedHashMap<>();
        while (messages.hasNext()) {
            Object message = messages.next();
            if (message instanceof GCNFragmentMessage) {
                fragmentMessages.add((GCNFragmentMessage) message);
            } else if (message instanceof GCNExpandMessage) {
                GCNExpandMessage expandMessage = (GCNExpandMessage) message;
                GCNExpandMessage existing = expandMessages.get(expandMessage.getRootId());
                if (existing == null || expandMessage.getDepth() < existing.getDepth()) {
                    expandMessages.put(expandMessage.getRootId(), expandMessage);
                }
            }
        }

        boolean stateChanged = false;
        if (state.getAccumulator() != null) {
            for (GCNFragmentMessage fragmentMessage : fragmentMessages) {
                if (vertexId.equals(fragmentMessage.getRootId())) {
                    state.getAccumulator().addFragment(fragmentMessage);
                    if (fragmentMessage.isDynamicExecution()) {
                        state.setDynamicExecution(true);
                    }
                    stateChanged = true;
                }
            }
        }

        for (GCNExpandMessage expandMessage : expandMessages.values()) {
            if (!state.shouldExpand(expandMessage.getRootId(), expandMessage.getDepth())) {
                continue;
            }
            GCNFragmentMessage localFragment = buildLocalFragment(expandMessage.getRootId(), vertex);
            context.sendMessage(expandMessage.getRootId(), localFragment);
            sendExpandMessages(expandMessage.getRootId(), expandMessage.getDepth(), vertexId,
                localFragment.getEdges());
            stateChanged = true;
        }

        if (stateChanged) {
            context.updateVertexValue(ObjectRow.create(state));
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        finishBatch(java.util.Collections.singletonList(graphVertex),
            java.util.Collections.singletonList(updatedValues));
    }

    @Override
    public void finishBatch(List<RowVertex> graphVertices, List<Optional<Row>> updatedValues) {
        if (!inferEnabled || graphVertices.isEmpty()) {
            if (!inferEnabled) {
                throw new IllegalStateException("GCN requires geaflow.infer.env.enable=true");
            }
            return;
        }
        List<RowVertex> batchVertices = new ArrayList<>(batchSize);
        List<Map<String, Object>> payloads = new ArrayList<>(batchSize);
        for (int i = 0; i < graphVertices.size(); i++) {
            RowVertex graphVertex = graphVertices.get(i);
            GCNState state = loadState(updatedValues.get(i));
            if (graphVertex == null || state.getAccumulator() == null) {
                continue;
            }
            batchVertices.add(graphVertex);
            payloads.add(payloadAssembler.assemble(graphVertex.getId(), state.getAccumulator()));
            if (payloads.size() >= batchSize) {
                emitBatch(batchVertices, payloads);
            }
        }
        emitBatch(batchVertices, payloads);
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("node_id", graphSchema.getIdType(), false),
            new TableField("embedding", new ArrayType(DoubleType.INSTANCE)),
            new TableField("prediction", IntegerType.INSTANCE),
            new TableField("confidence", DoubleType.INSTANCE)
        );
    }

    @Override
    public void finish() {
    }

    private void initializeWindowState(RowVertex vertex) {
        GCNState state = new GCNState();
        Object vertexId = vertex.getId();
        state.initAccumulator(vertexId);
        state.shouldExpand(vertexId, 0);
        GCNFragmentMessage localFragment = buildLocalFragment(vertexId, vertex);
        state.getAccumulator().addFragment(localFragment);
        state.setDynamicExecution(localFragment.isDynamicExecution());
        context.updateVertexValue(ObjectRow.create(state));
        sendExpandMessages(vertexId, 0, vertexId, localFragment.getEdges());
    }

    private void emitBatch(List<RowVertex> graphVertices, List<Map<String, Object>> payloads) {
        if (payloads.isEmpty()) {
            return;
        }
        List<Object> inferResults = modelContext.inferBatch(payloads);
        if (inferResults.size() != payloads.size()) {
            throw new IllegalArgumentException(String.format(
                "GCN infer batch result size mismatch, payloadSize=%s, resultSize=%s",
                payloads.size(), inferResults.size()));
        }
        for (int i = 0; i < inferResults.size(); i++) {
            RowVertex vertex = graphVertices.get(i);
            Row resultRow = resultDecoder.decode(vertex.getId(), inferResults.get(i));
            context.take(resultRow);
        }
        graphVertices.clear();
        payloads.clear();
    }

    private List<String> parseFields(String fieldConfig, String description) {
        List<String> fields = new ArrayList<>();
        for (String token : fieldConfig.split(",")) {
            String field = token.trim();
            if (!field.isEmpty()) {
                fields.add(field);
            }
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("No valid " + description + " configured");
        }
        return fields;
    }

    private Map<String, int[]> resolveVertexFeatureIndexes(GraphSchema graphSchema, List<String> fields) {
        Map<String, int[]> indexesByLabel = new LinkedHashMap<>();
        String resolvedSignature = null;
        for (TableField graphField : graphSchema.getFields()) {
            if (!(graphField.getType() instanceof VertexType)) {
                continue;
            }
            VertexType vertexType = (VertexType) graphField.getType();
            StructType valueType = new StructType(vertexType.getValueFields());
            int[] indexes = new int[fields.size()];
            StringBuilder signature = new StringBuilder();
            for (int i = 0; i < fields.size(); i++) {
                int index = valueType.indexOf(fields.get(i));
                if (index < 0) {
                    throw new IllegalArgumentException(String.format(
                        "GCN feature field '%s' missing from vertex label '%s'",
                        fields.get(i), graphField.getName()));
                }
                TableField field = valueType.getField(index);
                validateNumericField(field, "GCN feature field");
                indexes[i] = index;
                if (signature.length() > 0) {
                    signature.append(';');
                }
                signature.append(field.getName()).append(':')
                    .append(field.getType().getClass().getName());
            }
            String candidateSignature = signature.toString();
            if (resolvedSignature == null) {
                resolvedSignature = candidateSignature;
            } else if (!resolvedSignature.equals(candidateSignature)) {
                throw new IllegalArgumentException(
                    "GCN feature fields are inconsistent across vertex value schemas");
            }
            indexesByLabel.put(graphField.getName(), indexes);
        }
        if (indexesByLabel.isEmpty()) {
            throw new IllegalArgumentException("GCN requires at least one vertex schema");
        }
        return indexesByLabel;
    }

    private Map<String, Integer> resolveEdgeWeightIndexes(GraphSchema graphSchema, String weightField) {
        Map<String, Integer> indexesByLabel = new LinkedHashMap<>();
        if (weightField == null) {
            return indexesByLabel;
        }
        for (TableField graphField : graphSchema.getFields()) {
            if (!(graphField.getType() instanceof EdgeType)) {
                continue;
            }
            EdgeType edgeType = (EdgeType) graphField.getType();
            StructType valueType = new StructType(edgeType.getValueFields());
            int index = valueType.indexOf(weightField);
            if (index < 0) {
                throw new IllegalArgumentException(String.format(
                    "GCN edge weight field '%s' missing from edge label '%s'",
                    weightField, graphField.getName()));
            }
            TableField field = valueType.getField(index);
            validateNumericField(field, "GCN edge weight field");
            indexesByLabel.put(graphField.getName(), index);
        }
        return indexesByLabel;
    }

    private EdgeDirection parseEdgeDirection(String direction) {
        try {
            return EdgeDirection.valueOf(direction.trim().toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "geaflow.dsl.gcn.edge.direction must be one of IN, OUT, BOTH");
        }
    }

    private void validateNumericField(TableField field, String fieldType) {
        if (!isNumericScalarType(field.getType())) {
            throw new IllegalArgumentException(fieldType + " must be numeric scalar: "
                + field.getName());
        }
    }

    private boolean isNumericScalarType(IType<?> type) {
        return type instanceof ByteType
            || type instanceof ShortType
            || type instanceof IntegerType
            || type instanceof LongType
            || type instanceof FloatType
            || type instanceof DoubleType
            || type instanceof DecimalType;
    }

    private GCNState loadState(Optional<Row> updatedValues) {
        if (!updatedValues.isPresent()) {
            return new GCNState();
        }
        Object state = updatedValues.get().getField(0, ObjectType.INSTANCE);
        if (state instanceof GCNState) {
            return (GCNState) state;
        }
        return new GCNState();
    }

    private GCNFragmentMessage buildLocalFragment(Object rootId, RowVertex vertex) {
        Object vertexId = vertex.getId();
        List<RowEdge> mergedEdges = neighborhoodCollector.collectMergedEdges(vertexId,
            context.loadStaticEdges(edgeDirection), modelContext, edgeDirection, fanout);
        Row mergedValue = neighborhoodCollector.resolveMergedVertexValue(vertexId, vertex, modelContext);
        boolean dynamicOverlay = modelContext.loadDynamicVertexValue(vertexId) != null
            || !modelContext.loadDynamicEdges(vertexId, edgeDirection).isEmpty();
        List<Double> features = extractFeatures(vertex, mergedValue);
        List<GCNEdgeRecord> edgeRecords = new ArrayList<>(mergedEdges.size());
        for (RowEdge edge : mergedEdges) {
            edgeRecords.add(new GCNEdgeRecord(edge.getSrcId(), edge.getTargetId(), edge.getLabel(),
                edge.getDirect(), extractEdgeWeight(edge)));
        }
        return new GCNFragmentMessage(rootId, vertexId, features, edgeRecords, dynamicOverlay);
    }

    private List<Double> extractFeatures(RowVertex vertex, Row row) {
        int[] featureIndexes = featureIndexesByLabel.get(vertex.getLabel());
        if (featureIndexes == null) {
            throw new IllegalArgumentException("GCN vertex label is not supported: " + vertex.getLabel());
        }
        List<Double> features = new ArrayList<>(featureIndexes.length);
        if (row == null) {
            for (int i = 0; i < featureIndexes.length; i++) {
                features.add(0.0D);
            }
            return features;
        }
        for (int index : featureIndexes) {
            Object field = row.getField(index, ObjectType.INSTANCE);
            if (field == null) {
                features.add(0.0D);
            } else if (field instanceof Number) {
                features.add(((Number) field).doubleValue());
            } else {
                throw new IllegalArgumentException("GCN feature field value must be numeric: " + field);
            }
        }
        return features;
    }

    private double extractEdgeWeight(RowEdge edge) {
        if (!edgeWeightEnabled) {
            return 1.0D;
        }
        Integer index = edgeWeightIndexesByLabel.get(edge.getLabel());
        if (index == null) {
            throw new IllegalArgumentException("GCN edge label is not supported: " + edge.getLabel());
        }
        Row value = edge.getValue();
        if (value == null) {
            return 0.0D;
        }
        Object field = value.getField(index, ObjectType.INSTANCE);
        if (field == null) {
            return 0.0D;
        }
        if (field instanceof Number) {
            return ((Number) field).doubleValue();
        }
        throw new IllegalArgumentException("GCN edge weight field value must be numeric: " + field);
    }

    private void sendExpandMessages(Object rootId, int currentDepth, Object currentVertexId,
                                    List<GCNEdgeRecord> edgeRecords) {
        if (currentDepth >= hops) {
            return;
        }
        Set<Object> nextHop = new LinkedHashSet<>();
        for (GCNEdgeRecord edgeRecord : edgeRecords) {
            Object neighborId = currentVertexId.equals(edgeRecord.getSrcId())
                ? edgeRecord.getTargetId() : edgeRecord.getSrcId();
            if (neighborId == null || currentVertexId.equals(neighborId)) {
                continue;
            }
            nextHop.add(neighborId);
        }
        for (Object neighborId : nextHop) {
            context.sendMessage(neighborId, new GCNExpandMessage(rootId, currentDepth + 1));
        }
    }
}
