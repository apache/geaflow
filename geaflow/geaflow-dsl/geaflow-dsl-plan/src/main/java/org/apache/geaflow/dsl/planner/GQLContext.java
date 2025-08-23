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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.catalog.Catalog;
import org.apache.geaflow.dsl.catalog.CatalogFactory;
import org.apache.geaflow.dsl.catalog.CompileCatalog;
import org.apache.geaflow.dsl.catalog.GeaFlowRootCalciteSchema;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.common.descriptor.NodeDescriptor;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.optimize.GQLOptimizer;
import org.apache.geaflow.dsl.optimize.RuleGroup;
import org.apache.geaflow.dsl.parser.GQLConformance;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.rel.GQLToRelConverter;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.schema.GeaFlowView;
import org.apache.geaflow.dsl.schema.function.BuildInSqlFunctionTable;
import org.apache.geaflow.dsl.schema.function.BuildInSqlOperatorTable;
import org.apache.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.geaflow.dsl.sqlnode.SqlCreateTable;
import org.apache.geaflow.dsl.sqlnode.SqlCreateView;
import org.apache.geaflow.dsl.sqlnode.SqlEdge;
import org.apache.geaflow.dsl.sqlnode.SqlEdgeUsing;
import org.apache.geaflow.dsl.sqlnode.SqlTableColumn;
import org.apache.geaflow.dsl.sqlnode.SqlTableProperty;
import org.apache.geaflow.dsl.sqlnode.SqlVertex;
import org.apache.geaflow.dsl.sqlnode.SqlVertexUsing;
import org.apache.geaflow.dsl.util.GraphDescriptorUtil;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.apache.geaflow.dsl.util.StringLiteralUtil;
import org.apache.geaflow.dsl.validator.GQLValidatorImpl;
import org.apache.geaflow.dsl.validator.QueryNodeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GQLContext {

    static {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(GQLContext.class);

    private final Catalog catalog;

    private final SchemaPlus defaultSchema;

    private static final GQLConformance CONFORMANCE = GQLConformance.INSTANCE;

    private final GQLRelDataTypeSystem typeSystem = new GQLRelDataTypeSystem();

    private final GQLJavaTypeFactory typeFactory = new GQLJavaTypeFactory(typeSystem);

    private final GQLOperatorTable sqlOperatorTable;

    private final FrameworkConfig frameworkConfig;

    private final GQLRelBuilder relBuilder;

    private final GQLValidatorImpl validator;

    private final SqlRexConvertletTable convertLetTable;

    private String currentInstance;

    private String currentGraph;

    private final Set<SqlNode> validatedRelNode;

    private static final Map<String, String> shortKeyMapping = new HashMap<>();

    static {
        shortKeyMapping.put("storeType", DSLConfigKeys.GEAFLOW_DSL_STORE_TYPE.getKey());
        shortKeyMapping.put("shardCount", DSLConfigKeys.GEAFLOW_DSL_STORE_SHARD_COUNT.getKey());
        shortKeyMapping.put("type", DSLConfigKeys.GEAFLOW_DSL_TABLE_TYPE.getKey());
    }

    private GQLContext(Configuration conf, boolean isCompile) {
        this.currentInstance = conf.getString(DSLConfigKeys.GEAFLOW_DSL_CATALOG_INSTANCE_NAME);
        if (isCompile) {
            this.catalog = new CompileCatalog(CatalogFactory.getCatalog(conf));
        } else {
            this.catalog = CatalogFactory.getCatalog(conf);
        }
        this.defaultSchema = new GeaFlowRootCalciteSchema(this.catalog).plus();
        this.sqlOperatorTable = new GQLOperatorTable(
            catalog,
            typeFactory,
            this,
            new BuildInSqlOperatorTable(),
            new BuildInSqlFunctionTable(typeFactory));

        GQLCostFactory costFactory = new GQLCostFactory();
        this.frameworkConfig = Frameworks
            .newConfigBuilder()
            .defaultSchema(this.defaultSchema)
            .parserConfig(GeaFlowDSLParser.PARSER_CONFIG)
            .costFactory(costFactory)
            .typeSystem(this.typeSystem)
            .operatorTable(this.sqlOperatorTable)
            .build();

        this.relBuilder = GQLRelBuilder.create(frameworkConfig, createRexBuilder());
        CalciteCatalogReader calciteCatalogReader = createCatalogReader();

        this.validator = new GQLValidatorImpl(this, sqlOperatorTable,
            calciteCatalogReader, this.typeFactory, CONFORMANCE);
        this.validator.setIdentifierExpansion(true);
        this.convertLetTable = frameworkConfig.getConvertletTable();
        this.validatedRelNode = new HashSet<>();
    }

    public static GQLContext create(Configuration conf, boolean isCompile) {
        return new GQLContext(conf, isCompile);
    }

    public Catalog getCatalog() {
        return catalog;
    }

    /**
     * Convert {@link SqlCreateTable} to {@link GeaFlowTable}.
     */
    public GeaFlowTable convertToTable(SqlCreateTable table) {
        List<TableField> fields = Lists.newArrayList();

        for (SqlNode node : table.getColumns()) {
            SqlTableColumn columnNode = (SqlTableColumn) node;
            fields.add(columnNode.toTableField());
        }

        List<String> primaryFields = Lists.newArrayList();
        if (table.getPrimaryKeys() != null) {
            for (SqlNode node : table.getPrimaryKeys()) {
                SqlIdentifier primaryKey = (SqlIdentifier) node;
                primaryFields.add(primaryKey.getSimple());
            }
        }
        List<String> partitionFields = Lists.newArrayList();
        if (table.getPartitionFields() != null) {
            List<String> fieldNames = fields.stream()
                .map(TableField::getName).collect(Collectors.toList());

            for (SqlNode node : table.getPartitionFields()) {
                SqlIdentifier partitionField = (SqlIdentifier) node;
                partitionFields.add(partitionField.getSimple());
                int partitionIndex = fieldNames.indexOf(partitionField.getSimple());
                if (partitionIndex == -1) {
                    throw new GeaFlowDSLException(node.getParserPosition(),
                        "Partition field: {} is not exists in field list.", node);
                }
                if (partitionIndex < fieldNames.size() - partitionFields.size()) {
                    throw new GeaFlowDSLException(node.getParserPosition(),
                        "Partition field should be the last fields in the field list");
                }
            }
        }
        Map<String, String> config = Maps.newHashMap();
        if (table.getProperties() != null) {
            for (SqlNode sqlNode : table.getProperties()) {
                SqlTableProperty property = (SqlTableProperty) sqlNode;
                String key = keyMapping(property.getKey().toString());
                String value = StringLiteralUtil.toJavaString(property.getValue());
                config.put(key, value);
            }
        }
        String tableName = getCatalogObjName(table.getName());
        return new GeaFlowTable(currentInstance, tableName, fields, primaryFields, partitionFields,
            config, table.ifNotExists(), table.isTemporary());
    }

    public static String getCatalogObjName(SqlIdentifier name) {
        if (name.names.size() > 0) {
            return name.names.get(name.names.size() - 1);
        }
        throw new GeaFlowDSLException("Illegal table/graph/function name: " + name);
    }

    /**
     * Complete the catalog object name.
     *
     * @param name catalog object identifier.
     * @return completed catalog object identifier with instance name.
     */
    public SqlIdentifier completeCatalogObjName(SqlIdentifier name) {
        String firstName = name.names.get(0);
        if (!catalog.isInstanceExists(firstName)) {
            // if the first name is not an instance, append the current instance name.
            // e.g. table "user" in "select * from user" will complete to "${currentInstance}.user"
            List<String> newNames = new ArrayList<>();
            newNames.add(currentInstance);
            newNames.addAll(name.names);
            return new SqlIdentifier(newNames, name.getParserPosition());
        }
        return name;
    }

    /**
     * Convert {@link SqlCreateView} to {@link GeaFlowView}.
     */
    public GeaFlowView convertToView(SqlCreateView view) {
        String viewName = view.getName().getSimple();
        validator.validate(view.getSubQuery());
        validatedRelNode.add(view.getSubQuery());

        RelRecordType recordType = (RelRecordType) validator.getValidatedNodeType(view.getSubQuery());
        Preconditions.checkArgument(recordType.getFieldCount() == view.getFields().size(),
            "The column size of view " + viewName + " is " + view.getFields().size()
                + " ,but the output column size of the sub query is " + recordType.getFieldCount()
                + " at " + view.getParserPosition());

        List<String> fields = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();

        for (int i = 0; i < recordType.getFieldList().size(); i++) {
            String field = view.getFields().get(i).toString();
            RelDataType type = recordType.getFieldList().get(i).getType();
            fields.add(field);
            types.add(type);
        }
        RelDataType rowType = typeFactory.createStructType(types, fields);

        String viewSql = view.getSubQuerySql();

        return new GeaFlowView(currentInstance, viewName, fields, rowType, viewSql,
            view.ifNotExists());
    }

    /**
     * Convert {@link SqlCreateGraph} to {@link GeaFlowGraph}.
     */
    public GeaFlowGraph convertToGraph(SqlCreateGraph graph) {
        return convertToGraph(graph, Collections.emptyList());
    }

    public GeaFlowGraph convertToGraph(SqlCreateGraph graph,
                                       Collection<GeaFlowTable> createTablesInScript) {
        List<VertexTable> vertexTables = new ArrayList<>();
        SqlNodeList vertices = graph.getVertices();
        Map<String, String> vertexEdgeName2UsingTableNameMap = new HashMap<>();

        GraphDescriptor desc = new GraphDescriptor();
        for (SqlNode node : vertices) {
            String idFieldName = null;
            List<TableField> vertexFields = new ArrayList<>();

            if (node instanceof SqlVertex) {
                SqlVertex vertex = (SqlVertex) node;
                for (SqlNode column : vertex.getColumns()) {
                    SqlTableColumn tableColumn = (SqlTableColumn) column;
                    vertexFields.add(tableColumn.toTableField());
                    switch (tableColumn.getCategory()) {
                        case ID:
                            idFieldName = tableColumn.getName().getSimple();
                            break;
                        case NONE:
                            break;
                        default:
                            throw new GeaFlowDSLException("Illegal column category: " + tableColumn.getCategory()
                                + " at " + tableColumn.getParserPosition());
                    }
                }
                vertexTables.add(new VertexTable(currentInstance, vertex.getName().getSimple(),
                    vertexFields, idFieldName));
                desc.addNode(new NodeDescriptor(desc.getIdName(graph.getName().toString()),
                    vertex.getName().getSimple()));
            } else if (node instanceof SqlVertexUsing) {
                SqlVertexUsing vertexUsing = (SqlVertexUsing) node;
                List<String> names = vertexUsing.getUsingTableName().names;
                String tableName = vertexUsing.getUsingTableName().getSimple();

                Table usingTable = null;
                for (GeaFlowTable createTable : createTablesInScript) {
                    if (createTable.getName().equals(vertexUsing.getUsingTableName().getSimple())) {
                        usingTable = createTable;
                    }
                }
                if (usingTable == null) {
                    String instanceName = names.size() > 1 ? names.get(names.size() - 2) : getCurrentInstance();
                    usingTable = this.getCatalog().getTable(instanceName, tableName);
                }
                if (usingTable == null) {
                    throw new GeaFlowDSLException(node.getParserPosition(),
                        "Cannot found using table: {}, check statement order.", tableName);
                }
                idFieldName = vertexUsing.getId().getSimple();

                TableField idField = null;
                Set<String> fieldNames = new HashSet<>();
                for (RelDataTypeField column : usingTable.getRowType(this.typeFactory).getFieldList()) {
                    TableField tableField = new TableField(column.getName(),
                        SqlTypeUtil.convertType(column.getType()), column.getType().isNullable());
                    if (fieldNames.contains(tableField.getName())) {
                        throw new GeaFlowDSLException("Column already exists: {}", tableField.getName());
                    }
                    vertexFields.add(tableField);
                    fieldNames.add(tableField.getName());
                    if (tableField.getName().equals(idFieldName)) {
                        idField = tableField;
                    }
                }
                if (idField == null) {
                    throw new GeaFlowDSLException("Cannot found srcIdFieldName: {} in vertex {}",
                        idFieldName, vertexUsing.getName().getSimple());
                }
                vertexEdgeName2UsingTableNameMap.put(vertexUsing.getName().getSimple(),
                    vertexUsing.getUsingTableName().getSimple());
                vertexTables.add(new VertexTable(currentInstance, vertexUsing.getName().getSimple(),
                    vertexFields, idFieldName));
                desc.addNode(new NodeDescriptor(desc.getIdName(graph.getName().toString()),
                    vertexUsing.getName().getSimple()));
            } else {
                throw new GeaFlowDSLException("vertex not support: " + node);
            }
        }

        List<EdgeTable> edgeTables = new ArrayList<>();
        SqlNodeList edges = graph.getEdges();

        for (SqlNode node : edges) {
            String srcIdFieldName = null;
            String targetIdFieldName = null;
            String tsFieldName = null;
            List<TableField> edgeFields = new ArrayList<>();

            if (node instanceof SqlEdge) {
                SqlEdge edge = (SqlEdge) node;
                edge.validate();
                for (SqlNode column : edge.getColumns()) {
                    SqlTableColumn tableColumn = (SqlTableColumn) column;
                    if (tableColumn.getTypeFrom() != null) {
                        IType<?> columnType = null;
                        for (VertexTable vertexTable : vertexTables) {
                            if (vertexTable.getTypeName().equals(tableColumn.getTypeFrom().getSimple())) {
                                columnType = vertexTable.getIdField().getType();
                            }
                        }
                        assert columnType != null;
                        edgeFields.add(tableColumn.toTableField(columnType, false));
                    } else {
                        edgeFields.add(tableColumn.toTableField());
                    }
                    String columnName = tableColumn.getName().getSimple();

                    switch (tableColumn.getCategory()) {
                        case SOURCE_ID:
                            srcIdFieldName = columnName;
                            break;
                        case DESTINATION_ID:
                            targetIdFieldName = columnName;
                            break;
                        case TIMESTAMP:
                            tsFieldName = columnName;
                            break;
                        case NONE:
                            break;
                        default:
                            throw new GeaFlowDSLException("Illegal column category: " + tableColumn.getCategory()
                                + " at " + tableColumn.getParserPosition());
                    }
                }
                String tableName = edge.getName().getSimple();
                edgeTables.add(new EdgeTable(currentInstance, tableName, edgeFields, srcIdFieldName,
                    targetIdFieldName, tsFieldName));
                desc.addEdge(GraphDescriptorUtil.getEdgeDescriptor(desc, graph.getName().getSimple(), edge));
            } else if (node instanceof SqlEdgeUsing) {
                SqlEdgeUsing edgeUsing = (SqlEdgeUsing) node;
                List<String> names = edgeUsing.getUsingTableName().names;
                String tableName = edgeUsing.getUsingTableName().getSimple();

                Table usingTable = null;
                for (GeaFlowTable createTable : createTablesInScript) {
                    if (createTable.getName().equals(edgeUsing.getUsingTableName().getSimple())) {
                        usingTable = createTable;
                    }
                }
                if (usingTable == null) {
                    String instanceName = names.size() > 1 ? names.get(names.size() - 2) : getCurrentInstance();
                    usingTable = this.getCatalog().getTable(instanceName, tableName);
                }
                if (usingTable == null) {
                    throw new GeaFlowDSLException(node.getParserPosition(),
                        "Cannot found using table: {}, check statement order.", tableName);
                }

                srcIdFieldName = edgeUsing.getSourceId().getSimple();
                targetIdFieldName = edgeUsing.getTargetId().getSimple();
                tsFieldName = edgeUsing.getTimeField() == null ? null : edgeUsing.getTimeField().getSimple();
                TableField srcIdField = null;
                TableField targetIdField = null;
                TableField tsField = null;
                Set<String> fieldNames = new HashSet<>();
                for (RelDataTypeField column : usingTable.getRowType(this.typeFactory).getFieldList()) {
                    TableField tableField = new TableField(column.getName(),
                        SqlTypeUtil.convertType(column.getType()), column.getType().isNullable());
                    if (fieldNames.contains(tableField.getName())) {
                        throw new GeaFlowDSLException("Column already exists: {}", tableField.getName());
                    }
                    edgeFields.add(tableField);
                    fieldNames.add(tableField.getName());
                    if (tableField.getName().equals(srcIdFieldName)) {
                        srcIdField = tableField;
                    } else if (tableField.getName().equals(targetIdFieldName)) {
                        targetIdField = tableField;
                    } else if (tableField.getName().equals(tsFieldName)) {
                        tsField = tableField;
                    }
                }
                if (srcIdField == null) {
                    throw new GeaFlowDSLException("Cannot found srcIdFieldName: {} in edge {}",
                        srcIdFieldName, edgeUsing.getName().getSimple());
                }
                if (targetIdField == null) {
                    throw new GeaFlowDSLException("Cannot found targetIdFieldName: {} in edge {}",
                        targetIdFieldName, edgeUsing.getName().getSimple());
                }
                if (tsFieldName != null && tsField == null) {
                    throw new GeaFlowDSLException("Cannot found tsFieldName: {} in edge {}",
                        tsFieldName, edgeUsing.getName().getSimple());
                }
                vertexEdgeName2UsingTableNameMap.put(edgeUsing.getName().getSimple(),
                    edgeUsing.getUsingTableName().getSimple());
                edgeTables.add(new EdgeTable(currentInstance, edgeUsing.getName().getSimple(), edgeFields,
                    srcIdFieldName, targetIdFieldName, tsFieldName));
                desc.addEdge(
                    GraphDescriptorUtil.getEdgeDescriptor(desc, graph.getName().getSimple(), edgeUsing));
            }
        }

        Map<String, String> config = new HashMap<>();
        if (graph.getProperties() != null) {
            for (SqlNode sqlNode : graph.getProperties()) {
                SqlTableProperty property = (SqlTableProperty) sqlNode;
                String key = keyMapping(property.getKey().toString());
                String value = StringLiteralUtil.toJavaString(property.getValue());
                config.put(key, value);
            }
        }
        GeaFlowGraph geaFlowGraph = new GeaFlowGraph(currentInstance, graph.getName().getSimple(),
            vertexTables, edgeTables, config, vertexEdgeName2UsingTableNameMap, graph.ifNotExists(),
            graph.isTemporary(), desc);
        GraphDescriptor graphStats = geaFlowGraph.getValidDescriptorInGraph(desc);
        if (graphStats.nodes.size() != desc.nodes.size()
            || graphStats.edges.size() != desc.edges.size()
            || graphStats.relations.size() != desc.relations.size()) {
            throw new GeaFlowDSLException("Error occurred while generating desc as partially "
                + "constraints are invalid. \n desc: {} \n valid: {}",
                desc, graphStats);
        }
        geaFlowGraph.setDescriptor(graphStats);
        return geaFlowGraph;
    }

    private String keyMapping(String key) {
        return shortKeyMapping.getOrDefault(key, key);
    }

    public Map<String, String> keyMapping(Map<String, String> input) {
        Map<String, String> keyMapping = new HashMap<>();
        for (Map.Entry<String, String> entry : input.entrySet()) {
            keyMapping.put(keyMapping(entry.getKey()), entry.getValue());
        }
        return keyMapping;
    }

    /**
     * Register table to catalog.
     */
    public void registerTable(GeaFlowTable table) {
        String tableName = table.getName();
        catalog.createTable(currentInstance, table);
        LOG.info("register table : {} to catalog", tableName);
    }

    /**
     * Register view to catalog.
     */
    public void registerView(GeaFlowView view) {
        String tableName = view.getName();
        catalog.createView(currentInstance, view);
        LOG.info("register view : {} to catalog", tableName);
    }

    /**
     * Register graph to catalog.
     */
    public void registerGraph(GeaFlowGraph graph) {
        String graphName = graph.getName();
        catalog.createGraph(currentInstance, graph);
        LOG.info("register graph : {} to catalog", graphName);
    }

    public void registerFunction(GeaFlowFunction function) {
        sqlOperatorTable.registerSqlFunction(currentInstance, function);
        LOG.info("register Function : {} to catlog", function);
    }

    public SqlNode validate(SqlNode node) {
        if (validatedRelNode.contains(node)) {
            return node;
        }
        return validator.validate(node, new QueryNodeContext());
    }

    /**
     * Find the {@link SqlFunction}.
     */
    public SqlFunction findSqlFunction(String instance, String name) {
        return sqlOperatorTable.getSqlFunction(instance == null ? currentInstance : instance, name);
    }

    // ~ convert SqlNode to RelNode ----------------------------------------------------------

    /**
     * Return the RelRoot of SqlNode.
     *
     * @param sqlNode the sql node.
     * @return the rel root.
     */
    public RelNode toRelNode(SqlNode sqlNode) {
        RexBuilder rexBuilder = createRexBuilder();
        RelOptCluster cluster = RelOptCluster.create(relBuilder.getPlanner(), rexBuilder);

        SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
            .withTrimUnusedFields(false)
            .withInSubQueryThreshold(10000)
            .withConvertTableAccess(false)
            .build();

        GQLToRelConverter sqlToRelConverter = new GQLToRelConverter(
            new ViewExpanderImpl(),
            validator,
            createCatalogReader(),
            cluster,
            convertLetTable,
            config);

        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, false, true);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel));
        return root.rel;
    }

    private CalciteCatalogReader createCatalogReader() {
        SchemaPlus rootSchema = rootSchema(defaultSchema);
        List<String> defaultSchemaName = ImmutableList.of(defaultSchema.getName());
        Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            frameworkConfig.getParserConfig().caseSensitive());
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(properties);
        return new CalciteCatalogReader(CalciteSchema.from(rootSchema), defaultSchemaName, typeFactory, config);
    }

    private SchemaPlus rootSchema(SchemaPlus schema) {
        if (schema.getParentSchema() == null) {
            return schema;
        } else {
            return rootSchema(schema.getParentSchema());
        }
    }

    private RexBuilder createRexBuilder() {
        return new RexBuilder(typeFactory);
    }

    // RBO optimizer.
    public RelNode optimize(List<RuleGroup> ruleGroups, RelNode input) {
        GQLOptimizer optimizer = new GQLOptimizer(frameworkConfig.getContext());
        for (RuleGroup ruleGroup : ruleGroups) {
            optimizer.addRuleGroup(ruleGroup);
        }
        return optimizer.optimize(input);
    }

    // ~ CBO optimizer

    // Run VolcanoPlanner, transform by convention
    public RelNode transform(List<ConverterRule> ruleSet, RelNode relNode,
                             RelTraitSet relTraitSet) {
        Program optProgram = Programs.ofRules(ruleSet);
        RelNode transformed;
        try {
            transformed = optProgram.run(relBuilder.getPlanner(),
                relNode,
                relTraitSet,
                Lists.newArrayList(),
                Lists.newArrayList());
        } catch (RelOptPlanner.CannotPlanException e) {
            throw new GeaFlowDSLException(
                "Cannot generate a valid execution plan for the given query: \n\n" + RelOptUtil.toString(relNode)
                    + "This exception indicates that the query uses an unsupported SQL feature.\n"
                    + "Please check the documentation for the set create currently supported SQL features.", e);
        }
        return transformed;
    }

    /**
     * Gets framework config.
     *
     * @return the framework config
     */
    public FrameworkConfig getFrameworkConfig() {
        return frameworkConfig;
    }

    /**
     * Gets type factory.
     *
     * @return the type factory
     */
    public GQLJavaTypeFactory getTypeFactory() {
        return this.typeFactory;
    }

    public RelBuilder getRelBuilder() {
        return relBuilder;
    }

    private class ViewExpanderImpl implements RelOptTable.ViewExpander,
        Serializable {

        private static final long serialVersionUID = 42L;

        @Override
        public RelRoot expandView(RelDataType rowType,
                                  String queryString,
                                  List<String> schemaPath,
                                  List<String> viewPath) {

            SqlParser parser = SqlParser.create(queryString, GeaFlowDSLParser.PARSER_CONFIG);
            SqlNode sqlNode;
            try {
                sqlNode = parser.parseQuery();
            } catch (SqlParseException e) {
                throw new RuntimeException("parse failed", e);
            }
            CalciteCatalogReader reader = createCatalogReader()
                .withSchemaPath(schemaPath);
            SqlValidator validator = new GQLValidatorImpl(GQLContext.this, sqlOperatorTable,
                reader, typeFactory, CONFORMANCE);
            validator.setIdentifierExpansion(true);
            SqlNode validatedSqlNode = validator.validate(sqlNode);
            RexBuilder rexBuilder = createRexBuilder();
            RelOptCluster cluster = RelOptCluster.create(relBuilder.getPlanner(), rexBuilder);
            SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .build();
            SqlToRelConverter converter = new SqlToRelConverter(
                new ViewExpanderImpl(), validator, reader, cluster, convertLetTable, config);
            RelRoot root = converter.convertQuery(validatedSqlNode, true, false);
            root = root.withRel(converter.flattenTypes(root.rel, true));
            root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel));
            return root;
        }
    }

    public GQLValidatorImpl getValidator() {
        return validator;
    }

    public String getCurrentInstance() {
        return currentInstance;
    }

    public void setCurrentInstance(String currentInstance) {
        this.currentInstance = currentInstance;
    }

    public String getCurrentGraph() {
        return currentGraph;
    }

    public void setCurrentGraph(String currentGraph) {
        Table graphTable = catalog.getGraph(currentInstance, currentGraph);
        if (graphTable instanceof GeaFlowGraph) {
            this.currentGraph = currentGraph;
            GeaFlowGraph geaFlowGraph = (GeaFlowGraph) graphTable;
            geaFlowGraph.getConfig().putAll(keyMapping(geaFlowGraph.getConfig().getConfigMap()));
            getTypeFactory().setCurrentGraph(geaFlowGraph);
        } else {
            throw new GeaFlowDSLException("Graph: {} is not exists.", currentGraph);
        }

    }

    public boolean isCaseSensitive() {
        return validator.isCaseSensitive();
    }
}
