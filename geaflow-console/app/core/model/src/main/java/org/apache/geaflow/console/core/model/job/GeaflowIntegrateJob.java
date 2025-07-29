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

package org.apache.geaflow.console.core.model.job;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.VelocityUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowFieldCategory;
import org.apache.geaflow.console.common.util.type.GeaflowFieldCategory.NumConstraint;
import org.apache.geaflow.console.common.util.type.GeaflowJobType;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;
import org.apache.geaflow.console.core.model.code.GeaflowCode;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowStruct;
import org.apache.geaflow.console.core.model.data.GeaflowTable;
import org.apache.geaflow.console.core.model.data.GeaflowView;

public class GeaflowIntegrateJob extends GeaflowTransferJob {

    private static final String TEMPLATE = "template/integration.vm";

    public GeaflowIntegrateJob() {
        super(GeaflowJobType.INTEGRATE);
    }

    public void fromTableToGraph(GeaflowTable table, GeaflowGraph graph, GeaflowStructType type, String name,
                                 List<FieldMappingItem> fieldMapping) {
        super.addStructMapping(table, super.importGraphStruct(graph, type, name), fieldMapping);
    }

    public void fromViewToGraph(GeaflowView view, GeaflowGraph graph, GeaflowStructType type, String name,
                                Map<String, String> fieldMapping) {
        throw new GeaflowException("Unsupported operation");
    }

    @Override
    public GeaflowCode generateCode() {
        Map<String, Object> velocityMap = new HashMap<>();
        List<GeaflowGraph> graphs = getGraphs();
        String graphName = graphs.get(0).getName();
        List<Map<String, Object>> insertList = new ArrayList<>();

        for (StructMapping structMapping : structMappings) {
            String tableName = structMapping.getTableName();
            String structName = structMapping.getStructName();
            Map<String, Object> tableInsertMap = new HashMap<>();
            List<Map<String, String>> structs = new ArrayList<>();

            for (FieldMappingItem fieldMapping : structMapping.getFieldMappings()) {
                HashMap<String, String> map = new HashMap<>();
                map.put("structName", structName);
                map.put("tableFieldName", fieldMapping.getTableFieldName());
                map.put("structFieldName", fieldMapping.getStructFieldName());
                structs.add(map);
            }

            tableInsertMap.put("tableName", tableName);
            tableInsertMap.put("structs", structs);
            insertList.add(tableInsertMap);
        }
        velocityMap.put("graphName", graphName);
        velocityMap.put("inserts", insertList);
        String code = VelocityUtil.applyResource(TEMPLATE, velocityMap);
        return new GeaflowCode(code);
    }

    @Override
    public void validate() {
        super.validate();

        List<String> duplicates = checkDuplicates(structMappings, StructMapping::getStructName);
        Preconditions.checkArgument(CollectionUtils.isEmpty(duplicates),
            "Struct '%s' can only be integrated from one table.", String.join(",", duplicates));

        GeaflowGraph graph = new ArrayList<>(graphs.values()).get(0);
        Map<String, GeaflowStruct> graphStructMap = new HashMap<>();
        graphStructMap.putAll(graph.getEdges());
        graphStructMap.putAll(graph.getVertices());
        for (StructMapping structMapping : structMappings) {
            List<FieldMappingItem> fieldMappings = structMapping.getFieldMappings();
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(fieldMappings),
                "No fieldMapping from '%s' to '%s'.", structMapping.getTableName(), structMapping.getStructName());
            duplicates = checkDuplicates(fieldMappings, FieldMappingItem::getStructFieldName);
            Preconditions.checkArgument(CollectionUtils.isEmpty(duplicates), "Field '%s' can only be inserted once. (%s -> %s)",
                String.join(",", duplicates), structMapping.getTableName(), structMapping.getStructName());

            GeaflowStruct table = structs.get(structMapping.getTableName());
            GeaflowStruct struct = graphStructMap.get(structMapping.getStructName());

            for (FieldMappingItem item : fieldMappings) {
                String tableFieldName = item.getTableFieldName();
                String structFieldName = item.getStructFieldName();
                GeaflowField tableField = table.getFields().get(tableFieldName);
                GeaflowField structField = struct.getFields().get(structFieldName);
                Preconditions.checkNotNull(tableField, "Table '%s' has no field '%s'.", structMapping.getTableName(), tableFieldName);
                Preconditions.checkNotNull(structField, "Struct '%s' has no field '%s'.", structMapping.getStructName(), structFieldName);
                Preconditions.checkArgument(tableField.getType() == structField.getType(),
                    "Field type not match: %s (%s), %s (%s). (%s -> %s)",
                    tableFieldName, tableField.getType(), structFieldName, structField.getType(),
                    structMapping.getTableName(), structMapping.getStructName());
            }

            checkMetaFields(struct, fieldMappings);
        }
    }

    private <T> List<String> checkDuplicates(List<T> list, Function<T, String> nameFunction) {
        List<String> listNames = ListUtil.convert(list, nameFunction);
        List<String> distinctNames = listNames.stream().distinct().collect(Collectors.toList());
        return ListUtil.diff(listNames, distinctNames);
    }

    private void checkMetaFields(GeaflowStruct struct, List<FieldMappingItem> fieldMappings) {
        Set<GeaflowFieldCategory> mappingCategories = fieldMappings.stream()
            .map(e -> struct.getFields().get(e.getStructFieldName()).getCategory())
            .collect(Collectors.toSet());

        Set<GeaflowFieldCategory> structCategories = struct.getFields().values().stream().map(GeaflowField::getCategory)
            .collect(Collectors.toSet());
        // check required meta fields
        List<String> needFields = new ArrayList<>();
        for (GeaflowFieldCategory structCategory : structCategories) {
            if ((structCategory.getNumConstraint() == NumConstraint.EXACTLY_ONCE
                || structCategory.getNumConstraint() == NumConstraint.AT_MOST_ONCE)
                && !mappingCategories.contains(structCategory)) {
                needFields.add(structCategory.name());
            }
        }
        Preconditions.checkArgument(needFields.isEmpty(), "%s '%s' needs insert '%s' field", struct.getType(), struct.getName(),
            String.join(",", needFields));
    }

}
