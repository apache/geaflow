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

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.common.type.primitive.BinaryStringType;

public class SortInfo implements Serializable {

    public List<OrderByField> orderByFields = new ArrayList<>();

    public int fetch = -1;

    public SortInfo copy(List<OrderByField> orderByFields) {
        SortInfo sortInfo = new SortInfo();
        sortInfo.orderByFields = Lists.newArrayList(orderByFields);
        sortInfo.fetch = this.fetch;
        return sortInfo;
    }

    public boolean isRadixSortable() {
        for (int i = 0; i < this.orderByFields.size(); i++) {
            OrderByField field = this.orderByFields.get(i);
            IType<?> orderType = field.expression.getOutputType();
            if (Types.getType(orderType.getTypeClass()) != Types.INTEGER && Types.getType(orderType.getTypeClass()) != Types.BINARY_STRING) {
                return false;
            } else if (Types.getType(orderType.getTypeClass()) == Types.BINARY_STRING) {
                BinaryStringType bsType = (BinaryStringType)orderType;
                if (bsType.getPrecision() > 11 || bsType.getPrecision() < 0) {
                    return false;  
                }
            }
        }
        return true;
    }
}
