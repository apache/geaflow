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

package org.apache.geaflow.dsl.gremlin.parser;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * Represents a parsed Gremlin query.
 */
public class GremlinQuery implements Serializable {

    private final String queryString;
    private final Bytecode bytecode;
    private final Traversal traversal;

    public GremlinQuery(String queryString, Bytecode bytecode, Traversal traversal) {
        this.queryString = queryString;
        this.bytecode = bytecode;
        this.traversal = traversal;
    }

    /**
     * Get the original query string.
     *
     * @return the query string
     */
    public String getQueryString() {
        return queryString;
    }

    /**
     * Get the bytecode representation of the query.
     *
     * @return the bytecode
     */
    public Bytecode getBytecode() {
        return bytecode;
    }

    /**
     * Get the traversal representation of the query.
     *
     * @return the traversal
     */
    public Traversal getTraversal() {
        return traversal;
    }
    
    /**
     * Get the steps of the traversal.
     *
     * @return the list of steps
     */
    public List<Step> getSteps() {
        if (traversal instanceof GraphTraversal) {
            return ((GraphTraversal) traversal).asAdmin().getSteps();
        }
        return null;
    }
    
    /**
     * Get the number of steps in the traversal.
     *
     * @return the number of steps
     */
    public int getStepCount() {
        List<Step> steps = getSteps();
        return steps != null ? steps.size() : 0;
    }
    
    /**
     * Check if the query is valid.
     *
     * @return true if the query is valid, false otherwise
     */
    public boolean isValid() {
        return bytecode != null && traversal != null;
    }

    @Override
    public String toString() {
        return "GremlinQuery{"
            + "queryString='" + queryString + '\'' + ", "
            + "bytecode=" + bytecode + ", "
            + "traversal=" + traversal
            + '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GremlinQuery that = (GremlinQuery) o;
        return Objects.equals(queryString, that.queryString)
            && Objects.equals(bytecode, that.bytecode)
            && Objects.equals(traversal, that.traversal);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(queryString, bytecode, traversal);
    }
}