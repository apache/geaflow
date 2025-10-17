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

package org.apache.geaflow.dsl.gremlin.plan.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.geaflow.dsl.gremlin.parser.GremlinQuery;
import org.apache.geaflow.dsl.gremlin.plan.optimizer.GremlinQueryOptimizer;
import org.apache.geaflow.dsl.rel.GQLToRelConverter;
import org.apache.geaflow.dsl.rel.logical.LogicalGraphMatch;
import org.apache.geaflow.dsl.rel.match.EdgeMatch;
import org.apache.geaflow.dsl.rel.match.EdgeMatch.EdgeDirection;
import org.apache.geaflow.dsl.rel.match.SingleMatchNode;
import org.apache.geaflow.dsl.rel.match.VertexMatch;
import org.apache.geaflow.dsl.rel.match.VertexMatch.VertexType;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.rel.RelBuilder;
import org.apache.calcite.rel.RelBuilderFactory;
import org.apache.calcite.rel.core.RelFactories;
import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountStep;

/**
 * Implementation of GremlinToRelConverter for GeaFlow.
 */
public class GeaFlowGremlinToRelConverter implements GremlinToRelConverter {

    @Override
    public RelNode convert(GremlinQuery gremlinQuery, GQLToRelConverter gqlToRelConverter) {
        Traversal traversal = gremlinQuery.getTraversal();
        Bytecode bytecode = gremlinQuery.getBytecode();
        
        // Get the steps from the traversal
        List<Step> steps = traversal.asAdmin().getSteps();
        
        // Convert each step to RelNode
        RelNode currentRelNode = null;
        
        for (Step step : steps) {
            // Convert the step based on its type
            RelNode stepRelNode = convertStep(step, gqlToRelConverter, currentRelNode);
            if (currentRelNode == null) {
                currentRelNode = stepRelNode;
            } else {
                // Chain the RelNodes together
                currentRelNode = chainRelNodes(currentRelNode, stepRelNode);
            }
        }
        
        // Apply optimizations to the final plan
        if (currentRelNode != null) {
            currentRelNode = GremlinQueryOptimizer.optimize(currentRelNode);
        }
        
        return currentRelNode;
    }
    
    /**
     * Convert a Gremlin Step to a RelNode.
     *
     * @param step the Gremlin step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode in the chain
     * @return the converted RelNode
     */
    private RelNode convertStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Convert labels to a list for easier access
        List<String> labels = new ArrayList<>(step.getLabels());
        String stepName = labels.isEmpty() ? step.getClass().getSimpleName() : labels.get(0);
        
        // Handle different step types
        switch (stepName) {
            case "VStep":
                // Handle vertex step (g.V())
                return convertVertexStep(step, gqlToRelConverter);
            case "EStep":
                // Handle edge step (g.E())
                return convertEdgeStep(step, gqlToRelConverter);
            case "OutStep":
                // Handle out step (.out())
                return convertOutStep(step, gqlToRelConverter, currentRelNode);
            case "InStep":
                // Handle in step (.in())
                return convertInStep(step, gqlToRelConverter, currentRelNode);
            case "BothStep":
                // Handle both step (.both())
                return convertBothStep(step, gqlToRelConverter, currentRelNode);
            case "OutEStep":
                // Handle out edge step (.outE())
                return convertOutEStep(step, gqlToRelConverter, currentRelNode);
            case "InEStep":
                // Handle in edge step (.inE())
                return convertInEStep(step, gqlToRelConverter, currentRelNode);
            case "BothEStep":
                // Handle both edge step (.bothE())
                return convertBothEStep(step, gqlToRelConverter, currentRelNode);
            case "HasStep":
                // Handle has step (.has())
                return convertHasStep(step, gqlToRelConverter, currentRelNode);
            case "ValuesStep":
                // Handle values step (.values())
                return convertValuesStep(step, gqlToRelConverter, currentRelNode);
            case "ValueMapStep":
                // Handle value map step (.valueMap())
                return convertValueMapStep(step, gqlToRelConverter, currentRelNode);
            case "SelectStep":
                // Handle select step (.select())
                return convertSelectStep(step, gqlToRelConverter, currentRelNode);
            case "MapStep":
                // Handle map step (.map())
                return convertMapStep(step, gqlToRelConverter, currentRelNode);
            case "FlatMapStep":
                // Handle flat map step (.flatMap())
                return convertFlatMapStep(step, gqlToRelConverter, currentRelNode);
            case "FilterStep":
                // Handle filter step (.filter())
                return convertFilterStep(step, gqlToRelConverter, currentRelNode);
            case "WhereStep":
                // Handle where step (.where())
                return convertWhereStep(step, gqlToRelConverter, currentRelNode);
            case "PathStep":
                // Handle path step (.path())
                return convertPathStep(step, gqlToRelConverter, currentRelNode);
            case "CountStep":
                // Handle count step (.count())
                return convertCountStep(step, gqlToRelConverter, currentRelNode);
            case "SumStep":
                // Handle sum step (.sum())
                return convertSumStep(step, gqlToRelConverter, currentRelNode);
            case "MeanStep":
                // Handle mean step (.mean())
                return convertMeanStep(step, gqlToRelConverter, currentRelNode);
            case "GroupCountStep":
                // Handle group count step (.groupCount())
                return convertGroupCountStep(step, gqlToRelConverter, currentRelNode);
            case "OrderStep":
                // Handle order step (.order())
                return convertOrderStep(step, gqlToRelConverter, currentRelNode);
            case "LimitStep":
                // Handle limit step (.limit())
                return convertLimitStep(step, gqlToRelConverter, currentRelNode);
            case "RangeStep":
                // Handle range step (.range())
                return convertRangeStep(step, gqlToRelConverter, currentRelNode);
            default:
                // For unsupported steps, we create a generic step
                return convertGenericStep(step, gqlToRelConverter, currentRelNode);
        }
    }
    
    /**
     * Chain two RelNodes together.
     *
     * @param first the first RelNode
     * @param second the second RelNode
     * @return the chained RelNode
     */
    private RelNode chainRelNodes(RelNode first, RelNode second) {
        // If the second node is a GraphMatch, we need to merge it with the first if possible
        if (second instanceof GraphMatch && first instanceof GraphMatch) {
            GraphMatch firstMatch = (GraphMatch) first;
            GraphMatch secondMatch = (GraphMatch) second;
            
            // Try to merge the path patterns
            IMatchNode mergedPathPattern = mergePathPatterns(
                firstMatch.getPathPattern(), 
                secondMatch.getPathPattern()
            );
            
            if (mergedPathPattern != null) {
                // If we can merge, create a new GraphMatch with the merged path pattern
                return firstMatch.copy(firstMatch.getTraitSet(), firstMatch.getInput(), mergedPathPattern, mergedPathPattern.getRowType());
            } else {
                // If we can't merge, return the second node (this is a simplification)
                return second;
            }
        }
        // If the second node is a Filter, apply it to the first node
        else if (second instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) second;
            // Replace the filter's input with the first node
            return filter.copy(filter.getTraitSet(), ImmutableList.of(first), filter.getCondition());
        }
        // If the second node is a Project, apply it to the first node
        else if (second instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) second;
            // Replace the project's input with the first node
            return project.copy(project.getTraitSet(), ImmutableList.of(first), project.getProjects(), project.getRowType());
        }
        // If the second node is an Aggregate, apply it to the first node
        else if (second instanceof LogicalAggregate) {
            LogicalAggregate aggregate = (LogicalAggregate) second;
            // Replace the aggregate's input with the first node
            return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(first), aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCalls());
        }
        // For other cases, just return the second node (this is a simplification)
        else {
            return second;
        }
    }
    
    /**
     * Merge two path patterns if possible.
     *
     * @param first the first path pattern
     * @param second the second path pattern
     * @return the merged path pattern, or null if they can't be merged
     */
    private IMatchNode mergePathPatterns(IMatchNode first, IMatchNode second) {
        // This is a simplified implementation
        // In a real implementation, we would need to check if the path patterns can be merged
        // and then create a new merged path pattern
        
        // For now, we'll just return null to indicate that merging is not possible
        return null;
    }
    
    /**
     * Convert a vertex step (g.V()) to a RelNode.
     *
     * @param step the vertex step
     * @param gqlToRelConverter the GQL to Rel converter
     * @return the converted RelNode
     */
    private RelNode convertVertexStep(Step step, GQLToRelConverter gqlToRelConverter) {
        // Get the cluster from the GQL converter
        RelOptCluster cluster = gqlToRelConverter.getCluster();
        
        // Get the current graph from the GQL converter
        GeaFlowGraph graph = getCurrentGraph(gqlToRelConverter);
        
        // Check if the VStep has specific vertex IDs
        if (step instanceof GraphStep && ((GraphStep) step).getIds() != null && ((GraphStep) step).getIds().length > 0) {
            // Create a GraphScan with ID filtering
            LogicalGraphScan graphScan = LogicalGraphScan.create(cluster, graph);
            
            // Create filter condition for the vertex IDs
            RexBuilder rexBuilder = cluster.getRexBuilder();
            Object[] vertexIds = ((GraphStep) step).getIds();
            
            // Create filter condition for vertex IDs
            RexNode idCondition = createIdFilterCondition(rexBuilder, vertexIds);
            
            // Return a filtered GraphScan
            return org.apache.calcite.rel.logical.LogicalFilter.create(graphScan, idCondition);
        } else {
            // Create a full vertex scan
            return LogicalGraphScan.create(cluster, graph);
        }
    }
    
    /**
     * Get the current graph from the GQL converter.
     *
     * @param gqlToRelConverter the GQL to Rel converter
     * @return the current graph
     */
    private GeaFlowGraph getCurrentGraph(GQLToRelConverter gqlToRelConverter) {
        // This is a simplified implementation
        // In a real implementation, we would get the graph from the converter context
        return null; // Placeholder
    }
    
    /**
     * Create an ID filter condition for vertex/edge IDs.
     *
     * @param rexBuilder the RexBuilder
     * @param ids the IDs to filter by
     * @return the filter condition
     */
    private RexNode createIdFilterCondition(RexBuilder rexBuilder, Object[] ids) {
        if (ids.length == 1) {
            // Single ID filter
            RexLiteral idLiteral = rexBuilder.makeLiteral(ids[0]);
            // Assuming the ID field is at index 0, this would need to be adjusted based on the actual schema
            RexInputRef idRef = rexBuilder.makeInputRef(SqlTypeName.ANY, 0);
            return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS, idRef, idLiteral);
        } else {
            // Multiple ID filter using IN
            RexNode[] idLiterals = new RexNode[ids.length];
            for (int i = 0; i < ids.length; i++) {
                idLiterals[i] = rexBuilder.makeLiteral(ids[i]);
            }
            // Assuming the ID field is at index 0, this would need to be adjusted based on the actual schema
            RexInputRef idRef = rexBuilder.makeInputRef(SqlTypeName.ANY, 0);
            return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.OR, 
                Arrays.stream(idLiterals)
                    .map(literal -> rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS, idRef, literal))
                    .toArray(RexNode[]::new));
        }
    }
    
    /**
     * Convert an edge step (g.E()) to a RelNode.
     *
     * @param step the edge step
     * @param gqlToRelConverter the GQL to Rel converter
     * @return the converted RelNode
     */
    private RelNode convertEdgeStep(Step step, GQLToRelConverter gqlToRelConverter) {
        // Get the cluster from the GQL converter
        RelOptCluster cluster = gqlToRelConverter.getCluster();
        
        // Get the current graph from the GQL converter
        GeaFlowGraph graph = getCurrentGraph(gqlToRelConverter);
        
        // Check if the EStep has specific edge IDs
        if (step instanceof GraphStep && ((GraphStep) step).getIds() != null && ((GraphStep) step).getIds().length > 0) {
            // Create a GraphScan for edges with ID filtering
            LogicalGraphScan graphScan = LogicalGraphScan.create(cluster, graph);
            
            // Create filter condition for the edge IDs
            RexBuilder rexBuilder = cluster.getRexBuilder();
            Object[] edgeIds = ((GraphStep) step).getIds();
            
            // Create filter condition for edge IDs
            RexNode idCondition = createIdFilterCondition(rexBuilder, edgeIds);
            
            // Return a filtered GraphScan
            return org.apache.calcite.rel.logical.LogicalFilter.create(graphScan, idCondition);
        } else {
            // Create a full edge scan
            return LogicalGraphScan.create(cluster, graph);
        }
    }
    
    /**
     * Convert an out step (.out()) to a RelNode.
     *
     * @param step the out step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertOutStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Get edge labels and types from the step
        // Note: In a real implementation, we would extract this information from the step
        String edgeLabel = null; // Placeholder
        java.util.List<String> edgeTypes = Collections.emptyList(); // Placeholder
        
        // Create an edge match for outgoing edges
        EdgeMatch edgeMatch = EdgeMatch.create(
            gqlToRelConverter.getCluster(),
            currentRelNode instanceof GraphMatch ? (SingleMatchNode) ((GraphMatch) currentRelNode).getPathPattern() : null,
            edgeLabel,
            edgeTypes,
            EdgeDirection.OUT,
            null, // nodeType - would need to be determined from context
            null  // pathType - would need to be determined from context
        );
        
        // Create a GraphMatch with the edge match pattern
        return LogicalGraphMatch.create(
            gqlToRelConverter.getCluster(),
            currentRelNode,
            edgeMatch,
            edgeMatch.getPathSchema()
        );
    }
    
    /**
     * Convert an in step (.in()) to a RelNode.
     *
     * @param step the in step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertInStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Get edge labels and types from the step
        // Note: In a real implementation, we would extract this information from the step
        String edgeLabel = null; // Placeholder
        java.util.List<String> edgeTypes = Collections.emptyList(); // Placeholder
        
        // Create an edge match for incoming edges
        EdgeMatch edgeMatch = EdgeMatch.create(
            gqlToRelConverter.getCluster(),
            currentRelNode instanceof GraphMatch ? (SingleMatchNode) ((GraphMatch) currentRelNode).getPathPattern() : null,
            edgeLabel,
            edgeTypes,
            EdgeDirection.IN,
            null, // nodeType - would need to be determined from context
            null  // pathType - would need to be determined from context
        );
        
        // Create a GraphMatch with the edge match pattern
        return LogicalGraphMatch.create(
            gqlToRelConverter.getCluster(),
            currentRelNode,
            edgeMatch,
            edgeMatch.getPathSchema()
        );
    }
    
    /**
     * Convert a both step (.both()) to a RelNode.
     *
     * @param step the both step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertBothStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Get edge labels and types from the step
        // Note: In a real implementation, we would extract this information from the step
        String edgeLabel = null; // Placeholder
        java.util.List<String> edgeTypes = Collections.emptyList(); // Placeholder
        
        // Create an edge match for both directions
        EdgeMatch edgeMatch = EdgeMatch.create(
            gqlToRelConverter.getCluster(),
            currentRelNode instanceof GraphMatch ? (SingleMatchNode) ((GraphMatch) currentRelNode).getPathPattern() : null,
            edgeLabel,
            edgeTypes,
            EdgeDirection.BOTH,
            null, // nodeType - would need to be determined from context
            null  // pathType - would need to be determined from context
        );
        
        // Create a GraphMatch with the edge match pattern
        return LogicalGraphMatch.create(
            gqlToRelConverter.getCluster(),
            currentRelNode,
            edgeMatch,
            edgeMatch.getPathSchema()
        );
    }
    
    /**
     * Convert an out edge step (.outE()) to a RelNode.
     *
     * @param step the out edge step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertOutEStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for out edge step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert an in edge step (.inE()) to a RelNode.
     *
     * @param step the in edge step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertInEStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for in edge step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a both edge step (.bothE()) to a RelNode.
     *
     * @param step the both edge step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertBothEStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for both edge step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a has step (.has()) to a RelNode.
     *
     * @param step the has step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertHasStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        if (step instanceof HasStep) {
            HasStep hasStep = (HasStep) step;
            
            // Convert the HasStep condition to a RexNode
            RexNode condition = convertHasCondition(hasStep, gqlToRelConverter);
            
            // Create a filter node
            return LogicalFilter.create(currentRelNode, condition);
        }
        
        // If not a HasStep, return the current node unchanged
        return currentRelNode;
    }
    
    /**
     * Convert a HasStep condition to a RexNode.
     *
     * @param hasStep the HasStep
     * @param gqlToRelConverter the GQL to Rel converter
     * @return the RexNode representing the condition
     */
    private RexNode convertHasCondition(HasStep hasStep, GQLToRelConverter gqlToRelConverter) {
        // Get the key (property name) and predicate from the HasStep
        String key = hasStep.getKey();
        P predicate = hasStep.getPredicate();
        
        // Get the RexBuilder from the cluster
        RexBuilder rexBuilder = gqlToRelConverter.getCluster().getRexBuilder();
        
        // Create a RexNode for the value
        RexNode value = rexBuilder.makeLiteral(predicate.getValue());
        
        // Create a RexInputRef for the field (this is a simplification - in a real implementation
        // we would need to look up the field index based on the key)
        RexInputRef fieldRef = rexBuilder.makeInputRef(SqlTypeName.ANY, 0); // Placeholder index
        
        // Create the condition based on the predicate type
        switch (predicate.getBiPredicate().toString()) {
            case "eq":
                return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS, fieldRef, value);
            case "gt":
                return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, fieldRef, value);
            case "lt":
                return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, fieldRef, value);
            case "gte":
                return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, value);
            case "lte":
                return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL, fieldRef, value);
            default:
                throw new UnsupportedOperationException("Unsupported predicate: " + predicate.getBiPredicate());
        }
    }
    
    /**
     * Convert a values step (.values()) to a RelNode.
     *
     * @param step the values step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertValuesStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        if (step instanceof PropertiesStep) {
            PropertiesStep propertiesStep = (PropertiesStep) step;
            
            // Get the property keys to project
            String[] propertyKeys = propertiesStep.getPropertyKeys();
            
            // Create projection expressions
            List<RexNode> projects = new ArrayList<>();
            List<String> projectNames = new ArrayList<>();
            
            RexBuilder rexBuilder = gqlToRelConverter.getCluster().getRexBuilder();
            RelDataType rowType = currentRelNode.getRowType();
            
            // For each property key, create a projection expression
            for (String propertyKey : propertyKeys) {
                // Find the field index for the property key
                // This is a simplification - in a real implementation we would need to properly
                // look up the field based on the property key
                int fieldIndex = 0; // Placeholder - would need to be determined from the schema
                if (rowType.getField(propertyKey, false, false) != null) {
                    fieldIndex = rowType.getField(propertyKey, false, false).getIndex();
                }
                
                RexInputRef fieldRef = rexBuilder.makeInputRef(
                    rowType.getFieldList().get(fieldIndex).getType(), 
                    fieldIndex
                );
                projects.add(fieldRef);
                projectNames.add(propertyKey);
            }
            
            // Create a project node
            return LogicalProject.create(currentRelNode, projects, projectNames);
        }
        
        // If not a PropertiesStep, return the current node unchanged
        return currentRelNode;
    }
    
    /**
     * Convert a value map step (.valueMap()) to a RelNode.
     *
     * @param step the value map step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertValueMapStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for value map step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a select step (.select()) to a RelNode.
     *
     * @param step the select step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertSelectStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for select step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a map step (.map()) to a RelNode.
     *
     * @param step the map step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertMapStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for map step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a flat map step (.flatMap()) to a RelNode.
     *
     * @param step the flat map step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertFlatMapStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for flat map step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a filter step (.filter()) to a RelNode.
     *
     * @param step the filter step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertFilterStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for filter step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a where step (.where()) to a RelNode.
     *
     * @param step the where step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertWhereStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for where step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a path step (.path()) to a RelNode.
     *
     * @param step the path step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertPathStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for path step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a count step (.count()) to a RelNode.
     *
     * @param step the count step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertCountStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        if (step instanceof CountStep) {
            // Create a RelBuilder
            RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(gqlToRelConverter.getCluster(), null);
            
            // Create an aggregate call for COUNT(*)
            AggregateCall countCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false, // distinct
                false, // approximate
                false, // ignoreNulls
                ImmutableList.of(), // argList - empty for COUNT(*)
                -1, // filterArg
                null, // collation
                currentRelNode.getRowType().getFieldCount(), // groupCount
                currentRelNode, // input
                null, // type
                "count" // name
            );
            
            // Create an aggregate node with no grouping and the count call
            return LogicalAggregate.create(
                currentRelNode,
                ImmutableBitSet.of(), // groupSet - empty for no grouping
                null, // groupSets - null for no grouping sets
                ImmutableList.of(countCall) // aggCalls
            );
        }
        
        // If not a CountStep, return the current node unchanged
        return currentRelNode;
    }
    
    /**
     * Convert a sum step (.sum()) to a RelNode.
     *
     * @param step the sum step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertSumStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for sum step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a mean step (.mean()) to a RelNode.
     *
     * @param step the mean step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertMeanStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for mean step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a group count step (.groupCount()) to a RelNode.
     *
     * @param step the group count step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertGroupCountStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for group count step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert an order step (.order()) to a RelNode.
     *
     * @param step the order step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertOrderStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for order step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a limit step (.limit()) to a RelNode.
     *
     * @param step the limit step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertLimitStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for limit step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a range step (.range()) to a RelNode.
     *
     * @param step the range step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertRangeStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for range step conversion
        return null; // Placeholder
    }
    
    /**
     * Convert a generic step to a RelNode.
     *
     * @param step the generic step
     * @param gqlToRelConverter the GQL to Rel converter
     * @param currentRelNode the current RelNode
     * @return the converted RelNode
     */
    private RelNode convertGenericStep(Step step, GQLToRelConverter gqlToRelConverter, RelNode currentRelNode) {
        // Implementation for generic step conversion
        return null; // Placeholder
    }
}