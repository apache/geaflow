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

package org.apache.geaflow.dsl.runtime.function.table.order;

import java.util.Comparator;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;

public class TopNRowComparator<IN extends Row> implements Comparator<IN> {

    private final SortInfo sortInfo;

    public TopNRowComparator(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }

    public Comparator<IN> getNegativeComparator() {
        return new NegativeTopNRowComparator(this.sortInfo);
    }

    private class NegativeTopNRowComparator extends TopNRowComparator<IN> {

        public NegativeTopNRowComparator(SortInfo sortInfo) {
            super(sortInfo);
        }

        @Override
        public int compare(IN a, IN b) {
            return -super.compare(a, b);
        }
    }

    @Override
    public int compare(IN a, IN b) {
        List<OrderByField> fields = sortInfo.orderByFields;

        Object[] aOrders = new Object[fields.size()];
        Object[] bOrders = new Object[fields.size()];

        for (int i = 0; i < fields.size(); i++) {
            OrderByField field = fields.get(i);
            aOrders[i] = field.expression.evaluate(a);
            bOrders[i] = field.expression.evaluate(b);
            IType orderType = field.expression.getOutputType();
            int comparator = orderType.compare(aOrders[i], bOrders[i]);
            if (comparator != 0) {
                return comparator * (fields.get(i).order.value);
            }
        }
        return 0;
    }
}
