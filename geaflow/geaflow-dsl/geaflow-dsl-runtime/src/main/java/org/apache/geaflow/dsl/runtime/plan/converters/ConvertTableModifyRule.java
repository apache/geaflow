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

package org.apache.geaflow.dsl.runtime.plan.converters;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.geaflow.dsl.runtime.plan.PhysicConvention;
import org.apache.geaflow.dsl.runtime.plan.PhysicTableModifyRelNode;

public class ConvertTableModifyRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new ConvertTableModifyRule();

    private ConvertTableModifyRule() {
        super(LogicalTableModify.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertTableModifyRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode relNode) {
        LogicalTableModify modify = (LogicalTableModify) relNode;

        RelTraitSet relTraitSet = modify.getTraitSet().replace(PhysicConvention.INSTANCE);

        return new PhysicTableModifyRelNode(
            modify.getCluster(),
            relTraitSet,
            modify.getTable(),
            modify.getCatalogReader(),
            convert(modify.getInput(), relTraitSet),
            modify.getOperation(),
            modify.getUpdateColumnList(),
            modify.getSourceExpressionList(),
            modify.isFlattened());
    }

}
