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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.geaflow.common.config.ConfigHelper;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.infer.InferContext;
import org.apache.geaflow.infer.InferContextPool;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spatial Adaptive GNN (SA-GNN) algorithm from PaddleSpatial, integrated into GeaFlow
 * via the GQL CALL syntax.
 *
 * <p>SA-GNN is a graph neural network that incorporates spatial information (coordinates)
 * into graph convolution. Unlike GraphSAGE which uses direction-agnostic aggregation,
 * SA-GNN partitions neighbours into directional sectors based on spatial angles and
 * aggregates each sector independently, capturing richer spatial patterns.
 *
 * <p>GQL usage:
 * <pre>
 *   CALL SAGNN([numSamples, [numLayers]]) YIELD (vid, embedding)
 * </pre>
 *
 * <p>Feature vector convention:
 * The vertex feature vector sent to the Python model follows the convention used by
 * {@code SAGNNTransFormFunction}: the <b>last two elements</b> of the feature vector
 * are (coord_x, coord_y). All preceding elements are semantic node features.
 * If vertex features do not include spatial coordinates, the Python side will use
 * zero coordinates and SA-GNN will degrade gracefully to GCN-like aggregation.
 *
 * <p>Prerequisites (configuration keys):
 * <ul>
 *   <li>{@code geaflow.infer.env.enable = true}</li>
 *   <li>{@code geaflow.infer.framework.type = PADDLE}</li>
 *   <li>{@code geaflow.infer.env.user.transform.classname = SAGNNTransFormFunction}</li>
 *   <li>{@code geaflow.infer.env.conda.url = <miniconda installer URL>}</li>
 *   <li>Optionally: {@code geaflow.infer.env.paddle.gpu.enable = true}</li>
 * </ul>
 *
 * <p>Algorithm iterations:
 * <ol>
 *   <li>Iteration 1: For each vertex, sample up to {@code numSamples} neighbours and
 *       send own feature vector to each sampled neighbour.</li>
 *   <li>Iteration 2: Collect received features into the neighbour cache; send own
 *       features back to vertices that sampled this vertex.</li>
 *   <li>Iterations 3..numLayers+1: Call the Python SA-GNN model with the cached
 *       neighbour features. Store the resulting embedding in the vertex value.</li>
 * </ol>
 *
 * <p>Output: (vid, embedding_string) – one row per vertex.
 */
@Description(name = "sagnn", description = "built-in udga for PaddleSpatial SA-GNN node embedding")
public class SAGNN implements AlgorithmUserFunction<Object, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SAGNN.class);

    private AlgorithmRuntimeContext<Object, Object> context;
    private InferContext<List<Double>> inferContext;
    private FeatureReducer featureReducer;

    // ── Algorithm parameters ───────────────────────────────────────────────────

    /** Number of neighbours to sample per layer (default 10). */
    private int numSamples = 10;

    /** Number of SA-GNN layers (default 2). */
    private int numLayers = 2;

    /**
     * Total feature vector dimension expected by the Python model (default 64).
     * Includes semantic features AND the 2 coordinate dimensions at the end.
     * Tune this to match the SAGNNTransFormFunction.feature_dim setting.
     */
    private static final int TOTAL_FEATURE_DIM = 64;

    private static final Random RANDOM = new Random(42L);

    // ── Per-vertex state: neighbour feature cache ──────────────────────────────

    /**
     * Maps neighbour vertex ID → its feature vector (reduced + zero-padded if needed).
     * Populated in iteration 1 from messages; iterated over in later iterations.
     */
    private final Map<Object, List<Double>> neighbourFeatureCache = new HashMap<>();

    // ────────────────────────────────────────────────────────────────────────────
    // AlgorithmUserFunction interface
    // ────────────────────────────────────────────────────────────────────────────

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        this.context = context;

        if (parameters.length > 0) {
            this.numSamples = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            this.numLayers = Integer.parseInt(String.valueOf(parameters[1]));
        }
        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "SAGNN accepts at most 2 parameters: numSamples, numLayers. "
                    + "Usage: CALL SAGNN([numSamples, [numLayers]])");
        }

        // Feature reducer: keep all TOTAL_FEATURE_DIM dimensions (coordinates included).
        int[] dims = new int[TOTAL_FEATURE_DIM];
        for (int i = 0; i < TOTAL_FEATURE_DIM; i++) {
            dims[i] = i;
        }
        this.featureReducer = new FeatureReducer(dims);

        // Initialise Python inference context.
        try {
            boolean inferEnabled = ConfigHelper.getBooleanOrDefault(
                context.getConfig().getConfigMap(),
                FrameworkConfigKeys.INFER_ENV_ENABLE.getKey(),
                false);

            if (inferEnabled) {
                this.inferContext = InferContextPool.getOrCreate(context.getConfig());
                LOGGER.info(
                    "SAGNN initialised: numSamples={}, numLayers={}, inference={}",
                    numSamples, numLayers, InferContextPool.getStatus());
            } else {
                LOGGER.warn("SAGNN: inference environment not enabled. "
                    + "Set geaflow.infer.env.enable=true and "
                    + "geaflow.infer.framework.type=PADDLE.");
            }
        } catch (Exception e) {
            LOGGER.error("SAGNN: failed to initialise Python inference context", e);
            throw new RuntimeException("SAGNN requires Python inference environment: "
                + e.getMessage(), e);
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues,
                        Iterator<Object> messages) {
        updatedValues.ifPresent(vertex::setValue);

        long iter = context.getCurrentIterationId();
        Object vertexId = vertex.getId();

        if (iter == 1L) {
            // ── Iteration 1: sample neighbours and send own features ───────────
            List<RowEdge> outEdges = context.loadEdges(EdgeDirection.OUT);
            List<RowEdge> inEdges  = context.loadEdges(EdgeDirection.IN);

            List<RowEdge> allEdges = new ArrayList<>(outEdges.size() + inEdges.size());
            allEdges.addAll(outEdges);
            allEdges.addAll(inEdges);

            Map<Integer, List<Object>> sampledNeighbours = sampleNeighbours(vertexId, allEdges);

            // Persist sampled neighbours in vertex state for later iterations.
            Map<String, Object> vertexData = new HashMap<>();
            vertexData.put("sampledNeighbours", sampledNeighbours);
            context.updateVertexValue(ObjectRow.create(vertexData));

            // Send own feature vector to every sampled neighbour.
            List<Double> ownFeatures = getVertexFeatures(vertex);
            for (List<Object> layerNeighbours : sampledNeighbours.values()) {
                for (Object nbrId : layerNeighbours) {
                    Map<String, Object> msg = new HashMap<>();
                    msg.put("senderId", vertexId);
                    msg.put("features", ownFeatures);
                    context.sendMessage(nbrId, msg);
                }
            }

        } else if (iter == 2L) {
            // ── Iteration 2: collect neighbours' features; re-send own features ─
            consumeFeatureMessages(messages);

            List<Double> ownFeatures = getVertexFeatures(vertex);
            Map<Integer, List<Object>> sampledNeighbours = extractSampledNeighbours(vertex);
            if (sampledNeighbours != null) {
                for (List<Object> layerNeighbours : sampledNeighbours.values()) {
                    for (Object nbrId : layerNeighbours) {
                        Map<String, Object> msg = new HashMap<>();
                        msg.put("senderId", vertexId);
                        msg.put("features", ownFeatures);
                        context.sendMessage(nbrId, msg);
                    }
                }
            }

        } else if (iter <= numLayers + 1L) {
            // ── Iterations 3..numLayers+1: run SA-GNN inference ────────────────
            if (inferContext == null) {
                LOGGER.error("SAGNN: inference context not available for vertex {}", vertexId);
                return;
            }

            // Absorb any late-arriving feature messages.
            consumeFeatureMessages(messages);

            // Prepare vertex feature vector.
            List<Double> rawFeatures = getVertexFeatures(vertex);
            List<Double> vertexFeatures = padOrTruncate(rawFeatures, TOTAL_FEATURE_DIM);

            // Collect neighbour feature map (layer → list of feature vectors).
            Map<Integer, List<Object>> sampledNeighbours = extractSampledNeighbours(vertex);
            if (sampledNeighbours == null) {
                sampledNeighbours = new HashMap<>();
            }
            Map<Integer, List<List<Double>>> nbrFeaturesMap =
                collectNeighbourFeaturesMap(sampledNeighbours);

            // Call Python SA-GNN model.
            try {
                Object[] modelInputs = new Object[]{vertexId, vertexFeatures, nbrFeaturesMap};
                List<Double> embedding = inferContext.infer(modelInputs);

                Map<String, Object> result = new HashMap<>();
                result.put("embedding", embedding);
                context.updateVertexValue(ObjectRow.create(result));

            } catch (Exception e) {
                LOGGER.error("SAGNN: inference failed for vertex {}", vertexId, e);
                Map<String, Object> result = new HashMap<>();
                result.put("embedding", new ArrayList<Double>());
                context.updateVertexValue(ObjectRow.create(result));
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        if (!newValue.isPresent()) {
            return;
        }
        try {
            Object rawValue = vertex.getValue();
            Map<String, Object> data = extractMap(rawValue);
            if (data == null) {
                return;
            }
            @SuppressWarnings("unchecked")
            List<Double> embedding = (List<Double>) data.get("embedding");
            if (embedding != null && !embedding.isEmpty()) {
                context.take(ObjectRow.create(vertex.getId(), embedding.toString()));
            }
        } catch (Exception e) {
            LOGGER.error("SAGNN: finish failed for vertex {}", vertex.getId(), e);
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vid", graphSchema.getIdType(), false),
            new TableField("embedding",
                org.apache.geaflow.common.type.primitive.StringType.INSTANCE, false)
        );
    }

    @Override
    public void finish() {
        if (inferContext != null) {
            try {
                inferContext.close();
            } catch (Exception e) {
                LOGGER.warn("SAGNN: error closing inference context", e);
            }
        }
        neighbourFeatureCache.clear();
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ────────────────────────────────────────────────────────────────────────────

    /**
     * Sample up to {@code numSamples} neighbours per GNN layer from the edge list.
     * The same set of neighbours is reused across all layers (simple sampling strategy).
     */
    private Map<Integer, List<Object>> sampleNeighbours(
            Object vertexId, List<RowEdge> edges) {

        // Collect unique neighbour IDs (exclude self-loops).
        List<Object> allNeighbours = new ArrayList<>();
        for (RowEdge edge : edges) {
            Object nbrId = edge.getTargetId();
            if (!nbrId.equals(vertexId) && !allNeighbours.contains(nbrId)) {
                allNeighbours.add(nbrId);
            }
        }

        Map<Integer, List<Object>> result = new HashMap<>();
        for (int layer = 1; layer <= numLayers; layer++) {
            result.put(layer, sampleFixedSize(allNeighbours, numSamples));
        }
        return result;
    }

    /** Reservoir-style sampling with replacement (fixed seed for reproducibility). */
    private List<Object> sampleFixedSize(List<Object> pool, int size) {
        if (pool.isEmpty()) {
            return new ArrayList<>();
        }
        List<Object> sampled = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            sampled.add(pool.get(RANDOM.nextInt(pool.size())));
        }
        return sampled;
    }

    /**
     * Drain the message iterator and cache every received (senderId → features) pair.
     */
    @SuppressWarnings("unchecked")
    private void consumeFeatureMessages(Iterator<Object> messages) {
        while (messages.hasNext()) {
            Object msg = messages.next();
            if (msg instanceof Map) {
                Map<String, Object> msgMap = (Map<String, Object>) msg;
                Object senderId = msgMap.get("senderId");
                Object feats    = msgMap.get("features");
                if (senderId != null && feats instanceof List) {
                    neighbourFeatureCache.put(senderId, (List<Double>) feats);
                }
            }
        }
    }

    /**
     * Build the neighbour feature map (layer → list-of-feature-vectors) from the cache.
     */
    private Map<Integer, List<List<Double>>> collectNeighbourFeaturesMap(
            Map<Integer, List<Object>> sampledNeighbours) {

        Map<Integer, List<List<Double>>> result = new HashMap<>();
        for (Map.Entry<Integer, List<Object>> entry : sampledNeighbours.entrySet()) {
            int layer = entry.getKey();
            List<List<Double>> layerFeats = new ArrayList<>();
            for (Object nbrId : entry.getValue()) {
                List<Double> feat = neighbourFeatureCache.getOrDefault(nbrId, new ArrayList<>());
                layerFeats.add(padOrTruncate(feat, TOTAL_FEATURE_DIM));
            }
            result.put(layer, layerFeats);
        }
        return result;
    }

    /** Safely extract vertex features as a List<Double>. */
    @SuppressWarnings("unchecked")
    private List<Double> getVertexFeatures(RowVertex vertex) {
        Object val = vertex.getValue();
        if (val instanceof List) {
            return (List<Double>) val;
        }
        if (val instanceof Map) {
            Object feats = ((Map<?, ?>) val).get("features");
            if (feats instanceof List) {
                return (List<Double>) feats;
            }
        }
        return new ArrayList<>();
    }

    /** Safely extract the sampledNeighbours map stored in vertex state. */
    @SuppressWarnings("unchecked")
    private Map<Integer, List<Object>> extractSampledNeighbours(RowVertex vertex) {
        Map<String, Object> data = extractMap(vertex.getValue());
        if (data == null) {
            return null;
        }
        Object val = data.get("sampledNeighbours");
        if (val instanceof Map) {
            return (Map<Integer, List<Object>>) val;
        }
        return null;
    }

    /** Coerce an arbitrary object to Map<String, Object> if possible. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractMap(Object obj) {
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        if (obj instanceof Row) {
            try {
                return (Map<String, Object>) ((Row) obj).getField(0, ObjectType.INSTANCE);
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Ensure a feature vector has exactly {@code targetDim} elements by padding
     * with zeros or truncating.
     */
    private static List<Double> padOrTruncate(List<Double> features, int targetDim) {
        if (features == null) {
            features = new ArrayList<>();
        }
        List<Double> result = new ArrayList<>(targetDim);
        for (int i = 0; i < targetDim; i++) {
            result.add(i < features.size() ? features.get(i) : 0.0);
        }
        return result;
    }
}
