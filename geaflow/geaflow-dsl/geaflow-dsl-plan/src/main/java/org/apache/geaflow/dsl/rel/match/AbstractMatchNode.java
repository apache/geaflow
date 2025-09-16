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

package org.apache.geaflow.dsl.rel.match;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.geaflow.dsl.calcite.PathRecordType;

/**
 * Abstract base class for match nodes in GeaFlow.
 * This class provides common functionality for all match node implementations.
 */
public abstract class AbstractMatchNode extends AbstractRelNode implements IMatchNode {

    /**
     * The path schema for this match node.
     */
    protected final PathRecordType pathSchema;

    /**
     * Constructor for AbstractMatchNode.
     *
     * @param cluster the cluster
     * @param traits the trait set
     * @param pathSchema the path schema
     */
    protected AbstractMatchNode(RelOptCluster cluster, RelTraitSet traits, PathRecordType pathSchema) {
        super(cluster, traits);
        this.pathSchema = pathSchema;
        this.rowType = pathSchema;
    }

    @Override
    public PathRecordType getPathSchema() {
        return pathSchema;
    }
}
