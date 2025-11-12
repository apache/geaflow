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

package org.apache.geaflow.mcp.server.util;

import java.util.Locale;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowTable;

public class QueryFormatUtil {

    public static String makeResultTable(GeaFlowTable table, String resultPath) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE output_table(\n");
        int index = 0;
        for (TableField field : table.getFields()) {
            if (index > 0) {
                builder.append(",\n");
            }
            builder.append("`").append(field.getName()).append("` ")
                    .append(tableTypeMapper(field.getType().getName()));
            index++;
        }
        builder.append("\n) WITH ( \n");
        builder.append("type='file',\n");
        builder.append("geaflow.dsl.file.path='").append(resultPath).append("'\n");
        builder.append(");\n");
        return builder.toString();
    }

    public static String makeEntityTableQuery(GeaFlowTable table) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO output_table\n");
        if (table instanceof GeaFlowGraph.VertexTable) {
            builder.append("Match(a:`")
                    .append(((GeaFlowGraph.VertexTable)table).getTypeName())
                    .append("`)\n");
        } else {
            builder.append("Match()-[a:`")
                    .append(((GeaFlowGraph.EdgeTable)table).getTypeName())
                    .append("`]-()\n");
        }
        builder.append("Return ");
        int index = 0;
        for (TableField field : table.getFields()) {
            if (index > 0) {
                builder.append(",\n");
            }
            builder.append("a.`").append(field.getName()).append("` ");
            index++;
        }
        builder.append("\n;\n");
        return builder.toString();
    }

    public static String makeUseGraph(String graphName) {
        return "USE GRAPH " + graphName + ";\n";
    }

}
