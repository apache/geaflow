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

package org.apache.geaflow.processor.builder;

import com.google.common.base.Preconditions;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.operator.OpArgs.OpType;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.base.window.OneInputOperator;
import org.apache.geaflow.operator.base.window.TwoInputOperator;
import org.apache.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import org.apache.geaflow.operator.impl.io.WindowSourceOperator;
import org.apache.geaflow.processor.Processor;
import org.apache.geaflow.processor.impl.graph.GraphVertexCentricProcessor;
import org.apache.geaflow.processor.impl.io.SourceProcessor;
import org.apache.geaflow.processor.impl.window.OneInputProcessor;
import org.apache.geaflow.processor.impl.window.TwoInputProcessor;

public class ProcessorBuilder implements IProcessorBuilder {

    @Override
    public Processor buildProcessor(Operator operator) {
        Processor processor = null;
        OpType type = ((AbstractOperator) operator).getOpArgs().getOpType();
        String msg = String.format("operator %s type is null", operator);
        Preconditions.checkArgument(type != null, msg);
        switch (type) {
            case SINGLE_WINDOW_SOURCE:
            case MULTI_WINDOW_SOURCE:
                processor = new SourceProcessor((WindowSourceOperator) operator);
                break;
            case ONE_INPUT:
                processor = new OneInputProcessor((OneInputOperator) operator);
                break;
            case TWO_INPUT:
                processor = new TwoInputProcessor((TwoInputOperator) operator);
                break;
            case GRAPH_SOURCE:
                break;
            case VERTEX_CENTRIC_COMPUTE:
            case VERTEX_CENTRIC_COMPUTE_WITH_AGG:
            case VERTEX_CENTRIC_TRAVERSAL:
            case INC_VERTEX_CENTRIC_COMPUTE:
            case INC_VERTEX_CENTRIC_TRAVERSAL:
                processor = new GraphVertexCentricProcessor((AbstractGraphVertexCentricOp) operator);
                break;
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.operatorTypeNotSupportError(type.name()));
        }

        return processor;
    }
}

