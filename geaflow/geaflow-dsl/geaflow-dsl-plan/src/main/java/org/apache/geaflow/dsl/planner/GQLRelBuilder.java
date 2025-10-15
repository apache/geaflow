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

package org.apache.geaflow.dsl.planner;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

public class GQLRelBuilder extends RelBuilder {

    protected GQLRelBuilder(Context context,
                            RelOptCluster cluster,
                            RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }

    public static GQLRelBuilder create(FrameworkConfig config, RexBuilder builder) {

        final RelOptCluster[] clusters = new RelOptCluster[1];
        final RelOptSchema[] relOptSchemas = new RelOptSchema[1];

        Frameworks.withPrepare(
            new Frameworks.PrepareAction<Void>(config) {
                public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
                                  SchemaPlus rootSchema, CalciteServerStatement statement) {
                    clusters[0] = cluster;
                    relOptSchemas[0] = relOptSchema;
                    return null;
                }
            });
        RelOptCluster gqlCluster = RelOptCluster.create(clusters[0].getPlanner(), builder);
        return new GQLRelBuilder(config.getContext(), gqlCluster, relOptSchemas[0]);
    }

    public RelOptCluster getCluster() {
        return this.cluster;
    }

    public RelOptPlanner getPlanner() {
        return this.cluster.getPlanner();
    }

    public GQLJavaTypeFactory getTypeFactory() {
        return (GQLJavaTypeFactory) super.getTypeFactory();
    }
}
