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

package org.apache.geaflow.dsl.runtime.engine;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.IncVertexCentricTraversalFuncContext;
import org.apache.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.TemporaryGraph;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.state.KeyValueState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GeaFlowAlgorithmDynamicAggTraversalFunctionTest {

    @Test
    public void testEvolvePersistsNeighborhoodChangeVersion() throws Exception {
        GeaFlowAlgorithmDynamicAggTraversalFunction function =
            new GeaFlowAlgorithmDynamicAggTraversalFunction(mock(GraphSchema.class),
                mock(AlgorithmUserFunction.class), new Object[0]);
        IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext =
            mock(IncVertexCentricTraversalFuncContext.class, RETURNS_DEEP_STUBS);
        TemporaryGraph<Object, Row, Row> temporaryGraph = mock(TemporaryGraph.class);
        KeyValueState<Object, Long> changeVersions = mock(KeyValueState.class);
        when(traversalContext.getRuntimeContext().getWindowId()).thenReturn(7L);
        when(temporaryGraph.getEdges()).thenReturn(Collections.emptyList());
        when(changeVersions.get(2L)).thenReturn(5L);
        setField(function, "traversalContext", traversalContext);
        setField(function, "neighborhoodChangeVersions", changeVersions);

        function.evolve(2L, temporaryGraph);

        verify(changeVersions).put(2L, 7L);
        Assert.assertEquals(function.getNeighborhoodChangeVersion(2L), 5L);
    }

    @Test
    public void testMissingChangeVersionIsStatic() throws Exception {
        GeaFlowAlgorithmDynamicAggTraversalFunction function =
            new GeaFlowAlgorithmDynamicAggTraversalFunction(mock(GraphSchema.class),
                mock(AlgorithmUserFunction.class), new Object[0]);
        setField(function, "neighborhoodChangeVersions", mock(KeyValueState.class));

        Assert.assertEquals(function.getNeighborhoodChangeVersion(1L), Long.MIN_VALUE);
    }

    @Test
    public void testFinishCheckpointsNeighborhoodChangeVersions() throws Exception {
        AlgorithmUserFunction<Object, Object> userFunction = mock(AlgorithmUserFunction.class);
        GeaFlowAlgorithmDynamicAggTraversalFunction function =
            new GeaFlowAlgorithmDynamicAggTraversalFunction(mock(GraphSchema.class),
                userFunction, new Object[0]);
        IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext =
            mock(IncVertexCentricTraversalFuncContext.class, RETURNS_DEEP_STUBS);
        KeyValueState<Object, Row> vertexValues = mock(KeyValueState.class, RETURNS_DEEP_STUBS);
        KeyValueState<Object, Long> changeVersions = mock(KeyValueState.class, RETURNS_DEEP_STUBS);
        GeaFlowAlgorithmDynamicRuntimeContext algorithmContext =
            mock(GeaFlowAlgorithmDynamicRuntimeContext.class);
        when(traversalContext.getRuntimeContext().getWindowId()).thenReturn(7L);
        setField(function, "traversalContext", traversalContext);
        setField(function, "algorithmCtx", algorithmContext);
        setField(function, "initVertices", new HashSet<>());
        setField(function, "vertexUpdateValues", vertexValues);
        setField(function, "neighborhoodChangeVersions", changeVersions);

        function.finish();

        verify(changeVersions.manage().operate()).setCheckpointId(7L);
        verify(changeVersions.manage().operate()).finish();
        verify(changeVersions.manage().operate()).archive();
    }

    private void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
