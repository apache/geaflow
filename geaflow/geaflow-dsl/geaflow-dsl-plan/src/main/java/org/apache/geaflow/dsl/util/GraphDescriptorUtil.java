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

package org.apache.geaflow.dsl.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.collections.map.HashedMap;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.sqlnode.SqlEdge;
import org.apache.geaflow.dsl.sqlnode.SqlEdgeUsing;

public class GraphDescriptorUtil {

    public static List<EdgeDescriptor> getEdgeDescriptor(GraphDescriptor desc, String graphName, SqlEdge sqlEdge) {
        return getEdgeDescriptor(desc, graphName, sqlEdge.getName().getSimple(), sqlEdge.getConstraints());
    }

    public static List<EdgeDescriptor> getEdgeDescriptor(GraphDescriptor desc, String graphName, SqlEdgeUsing sqlEdgeUsing) {
        return getEdgeDescriptor(desc, graphName, sqlEdgeUsing.getName().getSimple(), sqlEdgeUsing.getConstraints());
    }

    private static List<EdgeDescriptor> getEdgeDescriptor(GraphDescriptor desc,
                                                          String graphName,
                                                          String edgeName,
                                                          SqlNodeList constraints) {
        List<EdgeDescriptor> result = new ArrayList<>();
        Map<String, List<String>> sourceType2TargetTypes = new HashedMap();
        for (Object obj : constraints) {
            assert obj instanceof GQLEdgeConstraint;
            GQLEdgeConstraint constraint = (GQLEdgeConstraint) obj;
            for (String sourceType : constraint.getSourceVertexTypes()) {
                sourceType2TargetTypes.computeIfAbsent(sourceType, t -> new ArrayList<>());
                for (String targetType : constraint.getTargetVertexTypes()) {
                    if (!sourceType2TargetTypes.get(sourceType).contains(targetType)) {
                        result.add(new EdgeDescriptor(desc.getIdName(graphName), edgeName,
                            sourceType, targetType));
                        sourceType2TargetTypes.get(sourceType).add(targetType);
                    }
                }
            }
        }
        return result;
    }

}
