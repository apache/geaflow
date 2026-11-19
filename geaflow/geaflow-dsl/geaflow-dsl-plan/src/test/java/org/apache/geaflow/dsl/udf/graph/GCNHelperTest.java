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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.ArrayType;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNAccumulator;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNEdgeRecord;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNFragmentMessage;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNPayloadAssembler;
import org.apache.geaflow.dsl.udf.graph.gcn.GCNResultDecoder;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.testng.annotations.Test;

public class GCNHelperTest {

    @Test
    public void testPayloadAssemblerWithBidirectionalAndSelfLoop() {
        GCNAccumulator accumulator = new GCNAccumulator(1L);
        accumulator.addFragment(new GCNFragmentMessage(1L, 1L,
            Arrays.asList(1.0, 2.0), Arrays.asList(
            new GCNEdgeRecord(1L, 2L, "knows", EdgeDirection.OUT, 1.0D)), false));
        accumulator.addFragment(new GCNFragmentMessage(1L, 2L,
            Arrays.asList(3.0, 4.0), Arrays.asList(
            new GCNEdgeRecord(2L, 1L, "knows", EdgeDirection.OUT, 1.0D)), false));

        GCNPayloadAssembler assembler = new GCNPayloadAssembler();
        Map<String, Object> payload = assembler.assemble(1L, accumulator);

        assertEquals(payload.get("center_node_id"), 1L);
        assertEquals(((List<?>) payload.get("sampled_nodes")).size(), 2);
        assertEquals(((List<?>) payload.get("node_features")).size(), 2);

        List<?> edgeIndex = (List<?>) payload.get("edge_index");
        List<?> row = (List<?>) edgeIndex.get(0);
        List<?> col = (List<?>) edgeIndex.get(1);
        assertEquals(row.size(), 4);
        assertEquals(col.size(), 4);
        assertEquals(row.get(0), 0);
        assertEquals(col.get(0), 1);
        assertEquals(row.get(1), 1);
        assertEquals(col.get(1), 0);
        assertEquals(row.get(2), 0);
        assertEquals(col.get(2), 0);
        assertEquals(row.get(3), 1);
        assertEquals(col.get(3), 1);

        List<?> edgeWeight = (List<?>) payload.get("edge_weight");
        assertEquals(edgeWeight.size(), 4);
        assertEquals(edgeWeight.get(0), 0.5D);
        assertEquals(edgeWeight.get(1), 0.5D);
        assertEquals(edgeWeight.get(2), 0.5D);
        assertEquals(edgeWeight.get(3), 0.5D);
    }

    @Test
    public void testAccumulatorOverwritesLatestFragmentPerVertex() {
        GCNAccumulator accumulator = new GCNAccumulator(1L);
        accumulator.addFragment(new GCNFragmentMessage(1L, 2L,
            Arrays.asList(1.0, 2.0), Arrays.asList(
            new GCNEdgeRecord(2L, 3L, "knows", EdgeDirection.OUT, 1.0D)), false));
        accumulator.addFragment(new GCNFragmentMessage(1L, 2L,
            Arrays.asList(9.0, 8.0), Arrays.asList(
            new GCNEdgeRecord(2L, 4L, "knows", EdgeDirection.OUT, 1.0D)), false));

        Map<Object, List<Double>> nodeFeatures = accumulator.getNodeFeatures();
        assertEquals(nodeFeatures.get(2L), Arrays.asList(9.0, 8.0));
        assertEquals(accumulator.getEdgeRecords().size(), 2);
    }

    @Test
    public void testPayloadAssemblerForDirectedSingleEdgeDoesNotInjectReverseEdge() {
        GCNAccumulator accumulator = new GCNAccumulator(1L);
        accumulator.addFragment(new GCNFragmentMessage(1L, 1L,
            Arrays.asList(1.0D), Arrays.asList(
            new GCNEdgeRecord(1L, 2L, "knows", EdgeDirection.OUT, 2.0D)), false));
        accumulator.addFragment(new GCNFragmentMessage(1L, 2L,
            Arrays.asList(2.0D), Collections.<GCNEdgeRecord>emptyList(), false));

        Map<String, Object> payload = new GCNPayloadAssembler().assemble(1L, accumulator);

        List<?> edgeIndex = (List<?>) payload.get("edge_index");
        assertEquals(edgeIndex.get(0), Arrays.asList(0, 0, 1));
        assertEquals(edgeIndex.get(1), Arrays.asList(1, 0, 1));
    }

    @Test
    public void testPayloadAssemblerAggregatesDuplicateDirectedEdges() {
        GCNAccumulator accumulator = new GCNAccumulator(1L);
        accumulator.addFragment(new GCNFragmentMessage(1L, 1L,
            Arrays.asList(1.0D), Arrays.asList(
            new GCNEdgeRecord(1L, 2L, "knows", EdgeDirection.OUT, 2.0D),
            new GCNEdgeRecord(1L, 2L, "knows", EdgeDirection.OUT, 3.0D)), false));
        accumulator.addFragment(new GCNFragmentMessage(1L, 2L,
            Arrays.asList(2.0D), Collections.<GCNEdgeRecord>emptyList(), false));

        Map<String, Object> payload = new GCNPayloadAssembler().assemble(1L, accumulator);

        List<?> edgeIndex = (List<?>) payload.get("edge_index");
        assertEquals(edgeIndex.get(0), Arrays.asList(0, 0, 1));
        assertEquals(edgeIndex.get(1), Arrays.asList(1, 0, 1));
        List<?> edgeWeight = (List<?>) payload.get("edge_weight");
        assertEquals(edgeWeight.get(0), 5.0D / Math.sqrt(6.0D));
    }

    @Test
    public void testResultDecoderWithMap() {
        Map<String, Object> inferResult = new HashMap<>();
        inferResult.put("node_id", 1L);
        inferResult.put("prediction", 7);
        inferResult.put("confidence", 0.9D);
        inferResult.put("embedding", Arrays.asList(0.1D, 0.2D));

        Row row = new GCNResultDecoder().decode(1L, inferResult);
        assertEquals(row.getField(0, LongType.INSTANCE), 1L);
        assertNotNull(row.getField(1, new ArrayType(DoubleType.INSTANCE)));
        Object[] embedding = (Object[]) row.getField(1, new ArrayType(DoubleType.INSTANCE));
        assertEquals(embedding.length, 2);
        assertEquals(row.getField(2, IntegerType.INSTANCE), 7);
        assertEquals(row.getField(3, DoubleType.INSTANCE), 0.9D);
    }

    @Test
    public void testResultDecoderUsesVertexIdWhenNodeIdMissingAndParsesNumericStrings() {
        Map<String, Object> inferResult = new HashMap<>();
        inferResult.put("prediction", "7");
        inferResult.put("confidence", "0.9");
        inferResult.put("embedding", Arrays.asList("0.1", 0.2D));

        Row row = new GCNResultDecoder().decode(5L, inferResult);

        assertEquals(row.getField(0, LongType.INSTANCE), 5L);
        assertEquals(row.getField(2, IntegerType.INSTANCE), Integer.valueOf(7));
        assertEquals(row.getField(3, DoubleType.INSTANCE), Double.valueOf(0.9D));
    }

    @Test
    public void testResultDecoderRejectsNodeIdMismatch() {
        Map<String, Object> inferResult = new HashMap<>();
        inferResult.put("node_id", 6L);

        assertThrows(IllegalArgumentException.class,
            () -> new GCNResultDecoder().decode(5L, inferResult));
    }

    @Test
    public void testResultDecoderReturnsNullEmbeddingForUnsupportedType() {
        Map<String, Object> inferResult = new HashMap<>();
        inferResult.put("embedding", "bad-embedding");

        Row row = new GCNResultDecoder().decode(5L, inferResult);

        assertNull(row.getField(1, new ArrayType(DoubleType.INSTANCE)));
    }

    @Test
    public void testResultDecoderRejectsNonMapInput() {
        GCNResultDecoder decoder = new GCNResultDecoder();
        try {
            decoder.decode(5L, "unexpected-result");
        } catch (IllegalArgumentException e) {
            assertNotNull(e.getMessage());
            return;
        }
        throw new AssertionError("expected exception");
    }
}
