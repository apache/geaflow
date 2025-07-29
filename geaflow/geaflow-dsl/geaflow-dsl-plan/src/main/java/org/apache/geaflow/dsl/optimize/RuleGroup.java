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

package org.apache.geaflow.dsl.optimize;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptRule;

public class RuleGroup implements Iterable<RelOptRule>, Comparable<RuleGroup> {

    public static final int DEFAULT_PRIORITY = 0;

    private final List<RelOptRule> rules;

    private final int priority;

    private RuleGroup(List<RelOptRule> rules, int priority) {
        this.rules = Objects.requireNonNull(rules);
        this.priority = priority;
    }

    public static RuleGroup of(List<RelOptRule> rules, int priority) {
        return new RuleGroup(rules, priority);
    }

    public RuleGroup(List<RelOptRule> rules) {
        this(rules, DEFAULT_PRIORITY);
    }

    @Override
    public Iterator<RelOptRule> iterator() {
        return rules.iterator();
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public int compareTo(RuleGroup o) {
        return Integer.compare(this.priority, o.priority);
    }

    public boolean isEmpty() {
        return rules.isEmpty();
    }
}
