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

package org.apache.geaflow.api.graph.base.algo;

import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;

public abstract class AbstractIncVertexCentricComputeAlgo<K, VV, EV, M,
    FUNC extends IncVertexCentricComputeFunction<K, VV, EV, M>> extends VertexCentricAlgo<K, VV, EV, M> {

    public AbstractIncVertexCentricComputeAlgo(long iterations) {
        super(iterations);
    }

    public AbstractIncVertexCentricComputeAlgo(long iterations, String name) {
        super(iterations, name);
    }

    public abstract FUNC getIncComputeFunction();

    /**
     * Returns the Python transform class name that this algorithm requires.
     *
     * <p>Override this method in subclasses to specify which Python UDF class
     * should be loaded for inference, enabling multiple algorithms with different
     * Python models to coexist in the same job without naming conflicts.
     *
     * <p>When this method returns a non-null value, the pipeline infrastructure
     * will create a dedicated {@code InferContext} keyed to that class name,
     * independent of the global {@code geaflow.infer.env.user.transform.classname}
     * configuration. When it returns {@code null} (the default), the global
     * configuration value is used.
     *
     * <p>Example:
     * <pre>
     *   // Using the default Python UDF specified in global config:
     *   incGraphView.incrementalCompute(new GraphSAGECompute(10, 2))
     *
     *   // Explicitly specifying a Python UDF (code-based approach):
     *   incGraphView.incrementalCompute(new GraphSAGECompute(10, 2, "GraphSAGETransFormFunction"))
     *
     *   // Two algorithms in the same job, each with its own Python UDF:
     *   incGraphView.incrementalCompute(new GraphSAGECompute(10, 2, "GraphSAGETransFormFunction"))
     *   incGraphView.incrementalCompute(new GCNCompute(64, "GCNTransFormFunction"))
     * </pre>
     *
     * @return the Python transform class name, or {@code null} to fall back to
     *         the global configuration
     */
    public String getPythonTransformClassName() {
        return null;
    }

}
