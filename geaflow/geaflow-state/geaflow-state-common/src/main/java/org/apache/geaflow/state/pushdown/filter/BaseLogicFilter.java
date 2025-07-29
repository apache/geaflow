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

package org.apache.geaflow.state.pushdown.filter;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.apache.geaflow.state.data.DataType;

public abstract class BaseLogicFilter implements IFilter {

    protected List<IFilter> filters = new ArrayList<>();
    protected BitSet rootBitSet = new BitSet();
    protected DataType dateType;

    public BaseLogicFilter(List<IFilter> inputs) {
        Preconditions.checkArgument(inputs != null && inputs.size() > 0);
        for (IFilter filter : inputs) {
            handleFilter(filter);
        }
    }

    public BaseLogicFilter(IFilter... inputs) {
        Preconditions.checkNotNull(inputs);
        for (IFilter filter : inputs) {
            handleFilter(filter);
        }
    }

    public boolean filter(Object value) {
        throw new UnsupportedOperationException();
    }

    public DataType dateType() {
        return dateType;
    }


    public List<IFilter> getFilters() {
        return filters;
    }

    public BitSet getRootBitSet() {
        return rootBitSet;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"").append(getFilterType()).append("\":[");
        if (filters != null && filters.size() > 0) {
            int size = filters.size();
            int index = 0;
            for (IFilter filter : filters) {
                sb.append(filter.toString());
                if (index < size - 1) {
                    sb.append(",");
                }
                index++;
            }
        }

        sb.append("]");
        sb.append("}");

        return sb.toString();
    }

    private void handleFilter(IFilter filter) {
        if (filter.getFilterType() == FilterType.EMPTY) {
            return;
        }

        sanityCheck(filter);
        if (filter.getFilterType() == getFilterType()) {
            filters.addAll(((BaseLogicFilter) filter).filters);
        } else {
            filters.add(filter);
        }
    }

    abstract void sanityCheck(IFilter filter);
}
