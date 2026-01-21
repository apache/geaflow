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

package org.apache.geaflow.ai.index.vector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KeywordVector implements IVector {

    private final String[] vec;

    public KeywordVector(String... vec) {
        this.vec = vec;
    }

    public String[] getVec() {
        return vec;
    }

    @Override
    public double match(IVector other) {
        if (!(other instanceof KeywordVector)) {
            return 0.0;
        }
        KeywordVector otherKeyword = (KeywordVector) other;
        String[] small = this.vec.length <= otherKeyword.vec.length ? this.vec : otherKeyword.vec;
        String[] large = this.vec.length <= otherKeyword.vec.length ? otherKeyword.vec : this.vec;
        Map<String, Long> keyword2TimesMap = new HashMap<>();
        for (String word : small) {
            keyword2TimesMap.put(word, keyword2TimesMap.getOrDefault(word, 0L) + 1);
        }
        int count = 0;
        for (String keyword : large) {
            if (keyword2TimesMap.containsKey(keyword)) {
                count += keyword2TimesMap.get(keyword);
            }
        }
        return count;
    }

    @Override
    public VectorType getType() {
        return VectorType.KeywordVector;
    }

    @Override
    public String toString() {
        return "KeywordVector{"
                + "vec=" + Arrays.toString(vec)
                + '}';
    }
}
