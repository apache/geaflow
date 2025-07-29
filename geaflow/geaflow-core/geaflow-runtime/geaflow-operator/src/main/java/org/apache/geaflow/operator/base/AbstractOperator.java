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

package org.apache.geaflow.operator.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.Function;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.RichWindowFunction;
import org.apache.geaflow.api.trait.CancellableTrait;
import org.apache.geaflow.collector.AbstractCollector;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.collector.chain.IChainCollector;
import org.apache.geaflow.collector.chain.OpChainCollector;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.metrics.common.MetricConstants;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.BlackHoleMetricGroup;
import org.apache.geaflow.metrics.common.api.Histogram;
import org.apache.geaflow.metrics.common.api.Meter;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.operator.OpArgs;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.utils.TicToc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOperator<FUNC extends Function> implements Operator, CancellableTrait {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOperator.class);

    private static final String ANONYMOUS = "Anonymous";
    private static final String EMPTY = "";

    protected OpArgs opArgs;
    protected FUNC function;

    protected List<ICollector> collectors;
    protected List<Operator> subOperatorList;
    protected Map<Integer, String> outputTags;
    protected boolean enableDebug;

    protected OpContext opContext;
    protected RuntimeContext runtimeContext;
    protected MetricGroup metricGroup;
    protected TicToc ticToc;
    protected Meter opInputMeter;
    protected Meter opOutputMeter;
    protected Histogram opRtHistogram;

    public AbstractOperator() {
        this.subOperatorList = new ArrayList<>();
        this.outputTags = new HashMap<>();
        this.opArgs = new OpArgs();
        this.enableDebug = false;
    }

    public AbstractOperator(FUNC function) {
        this();
        this.function = function;
    }

    @Override
    public void open(OpContext opContext) {
        this.opContext = opContext;
        Map<String, String> opConfig = opArgs.getConfig();
        this.runtimeContext = opContext.getRuntimeContext().clone(opConfig);
        boolean enableDetailMetric = this.runtimeContext
            .getConfiguration().getBoolean(ExecutionConfigKeys.ENABLE_DETAIL_METRIC);
        this.metricGroup = enableDetailMetric
            ? MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_FRAMEWORK)
            : BlackHoleMetricGroup.INSTANCE;
        this.opInputMeter = metricGroup.meter(MetricNameFormatter.inputTpsMetricName(this.getClass(), this.opArgs.getOpId()));
        this.opOutputMeter = metricGroup.meter(MetricNameFormatter.outputTpsMetricName(this.getClass(), this.opArgs.getOpId()));
        this.opRtHistogram = metricGroup.histogram(MetricNameFormatter.rtMetricName(this.getClass(), this.opArgs.getOpId()));
        this.ticToc = new TicToc();

        LOGGER.info("{} open,enableDebug:{}", this.getClass().getSimpleName(), enableDebug);

        this.collectors = new ArrayList<>();
        if (this.function instanceof RichFunction) {
            ((RichFunction) function).open(this.runtimeContext);
        }

        for (Operator subOperator : subOperatorList) {
            OpContext subOpContext = new DefaultOpContext(opContext.getCollectors(), opContext.getRuntimeContext());
            subOperator.open(subOpContext);
            IChainCollector<?> chainCollector = new OpChainCollector<>(opArgs.getOpId(), subOperator);
            this.collectors.add(chainCollector);
        }

        this.collectors.addAll(opContext.getCollectors().stream().filter(collector -> collector.getId() == opArgs.getOpId())
            .collect(Collectors.toList()));
        for (int i = 0, size = this.collectors.size(); i < size; i++) {
            ICollector<?> collector = this.collectors.get(i);
            collector.setUp(this.runtimeContext);
            if (collector instanceof AbstractCollector) {
                ((AbstractCollector) collector).setOutputMetric(this.opOutputMeter);
            }
        }

    }

    @Override
    public void close() {
        if (this.function instanceof RichFunction) {
            ((RichFunction) function).close();
        }
        for (Operator subOperator : subOperatorList) {
            subOperator.close();
        }
    }

    @Override
    public void finish() {
        if (this.function instanceof RichWindowFunction) {
            ((RichWindowFunction) function).finish();
        }
        for (int i = 0, size = this.collectors.size(); i < size; i++) {
            this.collectors.get(i).finish();
        }
        for (Operator operator : this.subOperatorList) {
            operator.finish();
        }
    }

    @Override
    public void cancel() {
        if (this.function instanceof CancellableTrait) {
            ((CancellableTrait) function).cancel();
        }
    }

    public OpArgs getOpArgs() {
        return opArgs;
    }

    public List<Operator> getNextOperators() {
        return this.subOperatorList;
    }

    public void addNextOperator(Operator operator) {
        this.subOperatorList.add(operator);
    }

    public Map<Integer, String> getOutputTags() {
        return outputTags;
    }

    public FUNC getFunction() {
        return function;
    }

    public void setFunction(FUNC function) {
        this.function = function;
    }

    public OpContext getOpContext() {
        return opContext;
    }

    @Override
    public String toString() {
        return getOperatorString(0);
    }

    /**
     * Returns display name of operator.
     */
    public String getOperatorString(int level) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < level; i++) {
            str.append("\t");
        }
        str.append(getClass().getSimpleName()).append("-").append(getIdentify()).append("-")
            .append(getFunctionString());
        for (Operator subOperator : subOperatorList) {
            str.append(((AbstractOperator) subOperator).getOperatorString(level + 1));
        }
        return str.toString();
    }

    public String getIdentify() {
        if (StringUtils.isNotBlank(opArgs.getOpName())) {
            return opArgs.getOpName();
        } else {
            return String.valueOf(opArgs.getOpId());
        }
    }

    private String getFunctionString() {
        if (function != null) {
            if (function.getClass().getSimpleName().length() == 0) {
                return ANONYMOUS;
            }
            return function.getClass().getSimpleName();
        }
        return EMPTY;
    }

    public static class DefaultOpContext implements OpContext {

        private final RuntimeContext runtimeContext;
        private final List<ICollector> collectors;

        public DefaultOpContext(List<ICollector> collectors, RuntimeContext runtimeContext) {
            this.runtimeContext = runtimeContext;
            this.collectors = collectors;
        }


        @Override
        public List<ICollector> getCollectors() {
            return this.collectors;
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return this.runtimeContext;
        }
    }
}
