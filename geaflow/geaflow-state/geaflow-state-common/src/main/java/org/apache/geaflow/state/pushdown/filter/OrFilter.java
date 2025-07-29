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
import java.util.BitSet;
import java.util.List;
import org.apache.geaflow.state.data.DataType;

public class OrFilter extends BaseLogicFilter {

    private static final BitSet EMPTY_BIT_SET = new BitSet();
    private boolean singleLimit;

    public OrFilter(List<IFilter> inputs) {
        super(inputs);
    }

    public OrFilter(IFilter... inputs) {
        super(inputs);
    }

    void sanityCheck(IFilter filter) {
        FilterType filterType = filter.getFilterType();
        DataType filterDataType = filter.dateType();

        Preconditions.checkArgument(!filterType.isRootFilter(), "filter illegal %s", filterType);
        if (this.dateType == null) {
            this.dateType = filterDataType;
        } else {
            Preconditions.checkArgument(this.dateType == filterDataType,
                "mix vertex filter and edge filter in OR list, %s, %s",
                this.dateType, filterDataType);
        }

        // root filters in or must be the same.
        BitSet filterRootBitSet = EMPTY_BIT_SET;
        if (filterType == FilterType.OR || filter.getFilterType() == FilterType.AND) {
            filterRootBitSet = ((BaseLogicFilter) filter).rootBitSet;
        }

        if (filters.size() == 0 && filterRootBitSet != EMPTY_BIT_SET) {
            this.rootBitSet = filterRootBitSet;
        } else if (filters.size() > 0) {
            Preconditions.checkArgument(this.rootBitSet.equals(filterRootBitSet));
        }
    }

    public OrFilter singleLimit() {
        this.singleLimit = true;
        return this;
    }

    public boolean isSingleLimit() {
        return singleLimit;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.OR;
    }
}
