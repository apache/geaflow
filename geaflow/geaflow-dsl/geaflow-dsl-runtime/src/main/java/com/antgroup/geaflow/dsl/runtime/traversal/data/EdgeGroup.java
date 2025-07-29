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

package com.antgroup.geaflow.dsl.runtime.traversal.data;

import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class EdgeGroup implements Iterable<RowEdge> {

    private final Iterable<RowEdge> edges;

    private EdgeGroup(Iterable<RowEdge> edges) {
        this.edges = edges;
    }

    public static EdgeGroup of(List<RowEdge> edges) {
        return new EdgeGroup(edges);
    }

    public static EdgeGroup of(Iterable<RowEdge> edges) {
        return new EdgeGroup(edges);
    }

    public static EdgeGroup of(Iterator<RowEdge> edges) {
        return new EdgeGroup(Lists.newArrayList(edges));
    }

    @Override
    public Iterator<RowEdge> iterator() {
        return edges.iterator();
    }

    public EdgeGroup filter(Predicate<RowEdge> predicate) {
        Iterable<RowEdge> filterEdges = Iterables.filter(edges, predicate::test);
        return EdgeGroup.of(filterEdges);
    }

    public EdgeGroup map(Function<RowEdge, RowEdge> function) {
        Iterable<RowEdge> mapEdges = Iterables.transform(edges, function::apply);
        return EdgeGroup.of(mapEdges);
    }

    public <E> Iterable<E> flatMap(Function<RowEdge, Iterator<E>> function) {
        Iterator<RowEdge> edgeIterator = edges.iterator();
        Iterator<E> flatIterator = new AbstractIterator<E>() {

            private Iterator<E> current = null;

            @Override
            protected E computeNext() {
                if (current == null || !current.hasNext()) {
                    if (edgeIterator.hasNext()) {
                        RowEdge edge = edges.iterator().next();
                        current = function.apply(edge);
                    } else {
                        current = null;
                    }
                }
                if (current == null) {
                    return this.endOfData();
                }
                return current.next();
            }
        };

        return new FluentIterable<E>() {
            @Override
            public Iterator<E> iterator() {
                return flatIterator;
            }
        };
    }
}
