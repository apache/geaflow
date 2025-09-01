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

package org.apache.geaflow.state.pushdown.inner;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ProtocolStringList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.pushdown.filter.FilterType;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.OrFilter;
import org.apache.geaflow.state.pushdown.filter.inner.EmptyGraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.GeneratedQueryFilter;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.OrGraphFilter;
import org.apache.geaflow.state.pushdown.inner.PushDownPb.FilterNode;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodeGenFilterConverter implements IFilterConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CodeGenFilterConverter.class);
    private static final String CODE_GEN_PACKAGE = "org.apache.geaflow.state.pushdown.filter.inner.";
    private static final String TEMPLATE_FILE_NAME = "Filter.template";
    private static final String FILTER_CLASS_HEADER = "GraphFilter_";
    private static final String TEMPLATE;
    private static final AtomicLong COUNTER = new AtomicLong(0);
    private static final int CACHE_SIZE = 1024;
    private static final Cache<FilterNode, IGraphFilter> FILTER_CACHE =
        CacheBuilder.newBuilder().initialCapacity(CACHE_SIZE).build();

    static {
        try (InputStream is = CodeGenFilterConverter.class.getClassLoader().getResourceAsStream(TEMPLATE_FILE_NAME)) {
            TEMPLATE = IOUtils.toString(is, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IFilter convert(IFilter origin) {
        if (origin.getFilterType() == FilterType.EMPTY) {
            return EmptyGraphFilter.of();
        }

        if (origin.getFilterType() == FilterType.OR && ((OrFilter) origin).isSingleLimit()) {
            List<IGraphFilter> list = ((OrFilter) origin).getFilters().stream()
                .map(f -> (IGraphFilter) innerConvert(f)).collect(Collectors.toList());
            return new OrGraphFilter(list);
        }
        return innerConvert(origin);
    }

    private IFilter innerConvert(IFilter origin) {
        try {
            FilterPlanWithData planWithData = FilterGenerator.getFilterPlanWithData(origin);
            IGraphFilter filter = FILTER_CACHE.getIfPresent(planWithData.plan);
            if (filter == null) {
                filter = (IGraphFilter) convert(planWithData.plan);
                FILTER_CACHE.put(planWithData.plan, filter);
            }
            IGraphFilter genFilter = filter.clone();
            VariableContext varContext = new VariableContext();
            variableGen(planWithData.data, varContext);
            ((GeneratedQueryFilter) genFilter).initVariables(varContext.variables.toArray(new Object[0]));
            return genFilter;
        } catch (Exception ex) {
            LOGGER.warn("code gen fail {}, return origin", ex.getMessage());
            return GraphFilter.of(origin);
        }
    }

    @Override
    public IFilter convert(FilterNode filterNode) {
        String className = FILTER_CLASS_HEADER + COUNTER.getAndIncrement();
        String src = codeGen(className, filterNode);
        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.cook(src);
            Class aClass = compiler.getClassLoader().loadClass(CODE_GEN_PACKAGE + className);
            return (IGraphFilter) aClass.newInstance();
        } catch (Exception e) {
            LOGGER.error("code gen compile fail\n{}", src);
            throw new RuntimeException(e);
        }
    }

    private String codeGen(String className, FilterNode filterNode) {
        CodeGenContext context = new CodeGenContext(new AtomicInteger(0));
        innerCodeGen(filterNode, context);
        return context.getCode(className);
    }

    private static void variableGen(FilterNode filterNode, VariableContext context) {
        if (filterNode.getFiltersCount() == 0) {
            switch (filterNode.getContentCase()) {
                case INT_CONTENT:
                    context.variables.addAll(filterNode.getIntContent().getIntList());
                    break;
                case BYTES_CONTENT:
                    context.variables.addAll(filterNode.getBytesContent().getBytesList());
                    break;
                case LONG_CONTENT:
                    context.variables.addAll(filterNode.getLongContent().getLongList());
                    break;
                case STR_CONTENT:
                    ProtocolStringList list = filterNode.getStrContent().getStrList();
                    PushDownPb.FilterType type = filterNode.getFilterType();
                    if (type == PushDownPb.FilterType.VERTEX_LABEL || type == PushDownPb.FilterType.EDGE_LABEL) {
                        context.variables.add(new HashSet<>(list));
                    } else {
                        context.variables.add(list);
                    }
                    break;
                default:
            }
        } else {
            List<FilterNode> filterNodes = filterNode.getFiltersList();
            for (FilterNode inNode : filterNodes) {
                variableGen(inNode, context);
            }
        }
    }

    private static void innerCodeGen(FilterNode filterNode, CodeGenContext context) {
        PushDownPb.FilterType type = filterNode.getFilterType();
        switch (type) {
            case AND:
                CodeGenContext inContext = new CodeGenContext(context.varIdx);
                for (FilterNode node : filterNode.getFiltersList()) {
                    innerCodeGen(node, inContext);
                }
                inContext.doAnd();
                context.merge(inContext);
                break;
            case OR:
                inContext = new CodeGenContext(context.varIdx);
                for (FilterNode node : filterNode.getFiltersList()) {
                    innerCodeGen(node, inContext);
                }
                inContext.doOr();
                context.merge(inContext);
                break;
            case VERTEX_LABEL:
                context.addVertexPreCompute(type);
                context.addVertexFormula(String.format("((Set<String>)var[%d]).contains(label)",
                    context.varIdx.getAndIncrement()));
                break;
            case EDGE_LABEL:
                context.addEdgePreCompute(type);
                context.addEdgeFormula(String.format("((Set<String>)var[%d]).contains(label)",
                    context.varIdx.getAndIncrement()));
                break;
            case VERTEX_TS:
                context.addVertexPreCompute(type);
                int start = context.varIdx.getAndIncrement();
                int end = context.varIdx.getAndIncrement();
                context.addVertexFormula(String.format("ts >= (Long)var[%d] && ts < (Long)var[%d]", start, end));
                break;
            case EDGE_TS:
                context.addEdgePreCompute(type);
                start = context.varIdx.getAndIncrement();
                end = context.varIdx.getAndIncrement();
                context.addEdgeFormula(String.format("ts >= (Long)var[%d] && ts < (Long)var[%d]", start, end));
                break;
            case IN_EDGE:
                context.addEdgeFormula("edge.getDirect() == EdgeDirection.IN");
                break;
            case OUT_EDGE:
                context.addEdgeFormula("edge.getDirect() == EdgeDirection.OUT");
                break;
            case VERTEX_VALUE_DROP:
                context.addVertexPreCompute(type);
                break;
            case EDGE_VALUE_DROP:
                context.addEdgePreCompute(type);
                break;
            case VERTEX_MUST_CONTAIN:
                context.addOneDegreeFormula("oneDegreeGraph.getVertex() != null");
                break;
            default:
                throw new GeaflowRuntimeException("not find type" + type);
        }
    }

    public static class CodeGenContext {

        private Set<PushDownPb.FilterType> edgePreFields = new HashSet<>();
        private Set<PushDownPb.FilterType> vertexPreFields = new HashSet<>();
        private List<String> edgeFormulas = new ArrayList<>();
        private List<String> vertexFormulas = new ArrayList<>();
        private List<String> oneDegreeFormulas = new ArrayList<>();
        private AtomicInteger varIdx;

        public CodeGenContext(AtomicInteger varIdx) {
            this.varIdx = varIdx;
        }

        private String getPreComputeCode(PushDownPb.FilterType type) {
            switch (type) {
                case VERTEX_TS:
                    return "long ts = ((IGraphElementWithTimeField)vertex).getTime();";
                case EDGE_TS:
                    return "long ts = ((IGraphElementWithTimeField)edge).getTime();";
                case VERTEX_LABEL:
                    return "String label = ((IGraphElementWithLabelField)vertex).getLabel();";
                case EDGE_LABEL:
                    return "String label = ((IGraphElementWithLabelField)edge).getLabel();";
                case VERTEX_VALUE_DROP:
                    return "vertex.withValue(null);";
                case EDGE_VALUE_DROP:
                    return "edge.withValue(null)";
                default:
                    throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
            }
        }

        public void addEdgeFormula(String code) {
            edgeFormulas.add(code);
        }

        public void addVertexFormula(String code) {
            vertexFormulas.add(code);
        }

        public void addEdgePreCompute(PushDownPb.FilterType type) {
            edgePreFields.add(type);
        }

        public void addVertexPreCompute(PushDownPb.FilterType type) {
            vertexPreFields.add(type);
        }

        public void doAnd() {
            doMerge(vertexFormulas, " && ");
            doMerge(edgeFormulas, " && ");
            doMerge(oneDegreeFormulas, " && ");
        }

        public void doOr() {
            doMerge(vertexFormulas, " || ");
            doMerge(edgeFormulas, " || ");
            doMerge(oneDegreeFormulas, " || ");
        }

        private void doMerge(List<String> formulas, String logic) {
            if (formulas.size() <= 1) {
                return;
            }
            StringBuilder mergedFormula = new StringBuilder();
            for (String formula : formulas) {
                mergedFormula.append("(").append(formula).append(")").append(logic);
            }
            formulas.clear();
            formulas.add(mergedFormula.substring(0, mergedFormula.length() - logic.length()));
        }

        public String getCode(String className) {
            Preconditions.checkArgument(vertexFormulas.size() <= 1);
            Preconditions.checkArgument(edgeFormulas.size() <= 1);

            String vertexCode = Boolean.TRUE.toString();
            String vertexPreCompute = "";
            if (vertexFormulas.size() == 1) {
                StringBuilder preCompute = new StringBuilder();
                for (PushDownPb.FilterType filterType : vertexPreFields) {
                    preCompute.append(getPreComputeCode(filterType)).append("\n");
                }
                vertexPreCompute = preCompute.toString();
                vertexCode = vertexFormulas.get(0);
            }
            String edgeCode = Boolean.TRUE.toString();
            String edgePreCompute = "";
            if (edgeFormulas.size() == 1) {
                StringBuilder preCompute = new StringBuilder();
                for (PushDownPb.FilterType filterType : edgePreFields) {
                    preCompute.append(getPreComputeCode(filterType)).append("\n");
                }
                edgePreCompute = preCompute.toString();
                edgeCode = edgeFormulas.get(0);
            }

            String oneDegreeCode = Boolean.TRUE.toString();
            if (oneDegreeFormulas.size() == 1) {
                oneDegreeCode = edgeFormulas.get(0);
            }

            return String.format(TEMPLATE, className, className, vertexPreCompute, vertexCode, edgePreCompute, edgeCode, oneDegreeCode);

        }

        public void merge(CodeGenContext inContext) {
            vertexPreFields.addAll(inContext.vertexPreFields);
            edgePreFields.addAll(inContext.edgePreFields);

            Preconditions.checkArgument(inContext.vertexFormulas.size() <= 1);
            Preconditions.checkArgument(inContext.edgeFormulas.size() <= 1);
            vertexFormulas.addAll(inContext.vertexFormulas);
            edgeFormulas.addAll(inContext.edgeFormulas);
        }

        public void addOneDegreeFormula(String formula) {
            oneDegreeFormulas.add(formula);
        }
    }

    public static class VariableContext {
        private List<Object> variables = new ArrayList<>();
    }
}
