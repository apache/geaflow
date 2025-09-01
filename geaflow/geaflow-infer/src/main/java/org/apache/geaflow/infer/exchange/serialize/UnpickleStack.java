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

package org.apache.geaflow.infer.exchange.serialize;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UnpickleStack implements Serializable {

    private static final long serialVersionUID = 5032718425413805422L;
    private final ArrayList<Object> stack;
    protected Object marker;

    public UnpickleStack() {
        stack = new ArrayList<>();
        marker = new Object();
    }

    public void add(Object o) {
        this.stack.add(o);
    }

    public void addMark() {
        this.stack.add(this.marker);
    }

    public Object pop() {
        int size = this.stack.size();
        Object result = this.stack.get(size - 1);
        this.stack.remove(size - 1);
        return result;
    }

    public List<Object> popAllSinceMarker() {
        ArrayList<Object> result = new ArrayList<>();
        Object o = pop();
        while (o != this.marker) {
            result.add(o);
            o = pop();
        }
        result.trimToSize();
        Collections.reverse(result);
        return result;
    }

    public Object peek() {
        return this.stack.get(this.stack.size() - 1);
    }

    public void trim() {
        this.stack.trimToSize();
    }

    public int size() {
        return this.stack.size();
    }

    public void clear() {
        this.stack.clear();
        this.stack.trimToSize();
    }
}
