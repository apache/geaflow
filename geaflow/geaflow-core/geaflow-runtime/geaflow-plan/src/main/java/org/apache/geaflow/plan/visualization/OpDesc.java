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

package org.apache.geaflow.plan.visualization;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;

public class OpDesc {

    private String name;
    private int id;
    private List<OpDesc> children;

    public OpDesc(Operator operator) {
        // Because of operator chaining, operator name should contains multiple operators.
        name = operator.toString();
        AbstractOperator abstractOperator = (AbstractOperator) operator;
        id = abstractOperator.getOpArgs().getOpId();
        children = new ArrayList<>();
        if (abstractOperator.getNextOperators() != null) {
            for (Object subOperator : abstractOperator.getNextOperators()) {
                children.add(new OpDesc((Operator) subOperator));
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
