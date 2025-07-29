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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.pdata.stream.window.PWindowSource;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.utils.ClassUtil;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.pushdown.EnablePartitionPushDown;
import org.apache.geaflow.dsl.common.pushdown.PartitionFilter;
import org.apache.geaflow.dsl.common.util.Windows;
import org.apache.geaflow.dsl.connector.api.ISkipOpenAndClose;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.TableReadableConnector;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.function.GeaFlowTableSourceFunction;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.QueryEngine;
import org.apache.geaflow.dsl.runtime.RuntimeGraph;
import org.apache.geaflow.dsl.runtime.RuntimeTable;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import org.apache.geaflow.dsl.runtime.expression.logic.AndExpression;
import org.apache.geaflow.dsl.runtime.plan.PhysicRelNode.PhysicRelNodeName;
import org.apache.geaflow.dsl.runtime.util.SchemaUtil;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.pipeline.job.IPipelineJobContext;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;

/**
 * GeaFlow's implement of the {@link QueryEngine}.
 */
public class GeaFlowQueryEngine implements QueryEngine {

    private final IPipelineJobContext pipelineContext;

    public GeaFlowQueryEngine(IPipelineJobContext pipelineContext) {
        this.pipelineContext = pipelineContext;
    }

    @Override
    public Map<String, String> getConfig() {
        return pipelineContext.getConfig().getConfigMap();
    }

    @Override
    public IPipelineJobContext getContext() {
        return pipelineContext;
    }

    @Override
    public RuntimeTable createRuntimeTable(QueryContext context, GeaFlowTable table, Expression pushFiler) {
        String tableType = table.getTableType();
        TableConnector connector = ConnectorFactory.loadConnector(tableType);
        if (!(connector instanceof TableReadableConnector)) {
            throw new GeaFlowDSLException("Table: '{}' is not readable", connector.getType());
        }
        TableReadableConnector readableConnector = (TableReadableConnector) connector;
        Configuration conf = table.getConfigWithGlobal(pipelineContext.getConfig(), context.getSetOptions());
        TableSource tableSource = readableConnector.createSource(conf);
        // push down filter to table source
        if (pushFiler != null) {
            List<Expression> filters = pushFiler.splitByAnd();
            List<Expression> partitionFilters = filters.stream()
                .filter(filter -> isPartitionFilter(filter, table))
                .collect(Collectors.toList());

            Expression partitionFilterExp =
                partitionFilters.isEmpty() ? LiteralExpression.createBoolean(true) :
                    new AndExpression(partitionFilters);
            PartitionFilter partitionFilter = new PartitionFilterImpl(partitionFilterExp,
                table.getPartitionIndices());
            if (tableSource instanceof EnablePartitionPushDown) {
                ((EnablePartitionPushDown) tableSource).setPartitionFilter(partitionFilter);
            }
        }

        tableSource.init(conf, table.getTableSchema());

        if (!(tableSource instanceof ISkipOpenAndClose)) {
            tableSource.open(new DefaultRuntimeContext(pipelineContext.getConfig()));
        }
        String opName = PhysicRelNodeName.TABLE_SCAN.getName(table.getName());
        int parallelism = context.getConfigParallelisms(opName, -1);
        if (parallelism == -1) {
            parallelism = Configuration.getInteger(DSLConfigKeys.GEAFLOW_DSL_TABLE_PARALLELISM,
                (Integer) DSLConfigKeys.GEAFLOW_DSL_TABLE_PARALLELISM.getDefaultValue(), table.getConfig());
        }
        int numPartitions = tableSource.listPartitions(parallelism).size();
        int partitionsPerParallelism = conf.getInteger(
            ConnectorConfigKeys.GEAFLOW_DSL_PARTITIONS_PER_SOURCE_PARALLELISM);
        int sourceParallelism;
        // If user has set source parallelism, use it.
        if (conf.contains(DSLConfigKeys.GEAFLOW_DSL_SOURCE_PARALLELISM)) {
            sourceParallelism = conf.getInteger(DSLConfigKeys.GEAFLOW_DSL_SOURCE_PARALLELISM);
        } else {
            if (numPartitions % partitionsPerParallelism > 0) {
                sourceParallelism = numPartitions / partitionsPerParallelism + 1;
            } else {
                sourceParallelism = numPartitions / partitionsPerParallelism;
            }
        }
        if (!(tableSource instanceof ISkipOpenAndClose)) {
            tableSource.close();
        }
        if (context.getConfigParallelisms(opName, sourceParallelism) == -1
            && Configuration.getInteger(DSLConfigKeys.GEAFLOW_DSL_TABLE_PARALLELISM, -1, table.getConfig()) == -1) {
            parallelism = sourceParallelism;
        }
        GeaFlowTableSourceFunction sourceFunction;
        if (conf.contains(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION)) {
            String customClassName = conf.getString(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION);
            try {
                sourceFunction = (GeaFlowTableSourceFunction) ClassUtil.classForName(customClassName)
                    .getConstructor(GeaFlowTable.class, TableSource.class)
                    .newInstance(table, tableSource);
            } catch (Exception e) {
                throw new GeaFlowDSLException("Cannot create sink function: {}.",
                    customClassName, e);
            }
        } else {
            sourceFunction = new GeaFlowTableSourceFunction(table, tableSource);
        }

        IWindow<Row> window = Windows.createWindow(conf);
        PWindowSource<Row> source = pipelineContext.buildSource(sourceFunction, window)
            .withConfig(context.getSetOptions())
            .withName(opName)
            .withParallelism(parallelism);

        return new GeaFlowRuntimeTable(context, pipelineContext, source);
    }

    @Override
    public RuntimeTable createRuntimeTable(QueryContext context, Collection<Row> rows) {
        IWindow<Row> window = Windows.createWindow(pipelineContext.getConfig());
        PWindowSource<Row> source = pipelineContext.buildSource(new CollectionSource<>(rows), window)
            .withConfig(context.getSetOptions())
            .withName(PhysicRelNodeName.VALUE_SCAN.getName(context.getOpNameCount()))
            .withParallelism(1);
        return new GeaFlowRuntimeTable(context, pipelineContext, source);
    }

    @Override
    public <T> PWindowSource<T> createRuntimeTable(QueryContext context, SourceFunction<T> sourceFunction) {
        IWindow<T> window = Windows.createWindow(pipelineContext.getConfig());
        return pipelineContext.buildSource(sourceFunction, window)
            .withConfig(context.getSetOptions());
    }

    @SuppressWarnings("unchecked")
    @Override
    public RuntimeGraph createRuntimeGraph(QueryContext context, GeaFlowGraph graph) {
        GraphViewDesc graphViewDesc = SchemaUtil.buildGraphViewDesc(graph, context.getGlobalConf());
        PGraphView<Object, Row, Row> pGraphView = pipelineContext.createGraphView(graphViewDesc);
        return new GeaFlowRuntimeGraph(context, pGraphView, graph, graphViewDesc);
    }

    public IPipelineJobContext getPipelineContext() {
        return pipelineContext;
    }

    private boolean isPartitionFilter(Expression filter, GeaFlowTable table) {
        if (filter.getInputs().size() != 2) {
            return false;
        }
        Expression left = filter.getInputs().get(0);
        Expression right = filter.getInputs().get(1);
        if (left instanceof FieldExpression
            && table.isPartitionField(((FieldExpression) left).getFieldIndex())
            && right instanceof LiteralExpression) {
            return true;
        }
        return right instanceof FieldExpression
            && table.isPartitionField(((FieldExpression) right).getFieldIndex())
            && left instanceof LiteralExpression;
    }

    private static class PartitionFilterImpl implements PartitionFilter {

        private final Expression filter;

        public PartitionFilterImpl(Expression filter, List<Integer> partitionFields) {
            this.filter = filter.replace(exp -> {
                if (exp instanceof FieldExpression) {
                    FieldExpression field = (FieldExpression) exp;
                    int indexOfPartitionField = partitionFields.indexOf(field.getFieldIndex());
                    assert indexOfPartitionField >= 0 : "Not a partition field in partition filter";
                    return field.copy(indexOfPartitionField);
                }
                return exp;
            });
        }

        @Override
        public boolean apply(Row partition) {
            Boolean accept = (Boolean) filter.evaluate(partition);
            return accept != null && accept;
        }
    }
}
