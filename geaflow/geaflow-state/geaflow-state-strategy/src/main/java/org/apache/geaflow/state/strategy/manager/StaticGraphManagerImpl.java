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

package org.apache.geaflow.state.strategy.manager;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.state.context.StateContext;
import org.apache.geaflow.state.data.OneDegreeGraph;
import org.apache.geaflow.state.graph.StaticGraphTrait;
import org.apache.geaflow.state.iterator.IteratorWithFilter;
import org.apache.geaflow.state.iterator.MultiIterator;
import org.apache.geaflow.state.pushdown.IStatePushDown;
import org.apache.geaflow.state.strategy.accessor.IAccessor;
import org.apache.geaflow.utils.keygroup.KeyGroup;

public class StaticGraphManagerImpl<K, VV, EV> extends BaseShardManager<K,
    StaticGraphTrait<K, VV, EV>> implements StaticGraphTrait<K, VV, EV> {


    public StaticGraphManagerImpl(StateContext context, Map<Integer, IAccessor> accessorMap) {
        super(context, accessorMap);
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        getTraitByKey(edge.getSrcId()).addEdge(edge);
    }

    @Override
    public List<IEdge<K, EV>> getEdges(K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getEdges(sid, pushdown);
    }

    @Override
    public OneDegreeGraph<K, VV, EV> getOneDegreeGraph(K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getOneDegreeGraph(sid, pushdown);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        getTraitByKey(vertex.getId()).addVertex(vertex);
    }

    @Override
    public IVertex<K, VV> getVertex(K sid, IStatePushDown pushdown) {
        return getTraitByKey(sid).getVertex(sid, pushdown);
    }

    @Override
    public CloseableIterator<K> vertexIDIterator() {
        List<CloseableIterator<K>> iterators = new ArrayList<>();
        for (Entry<Integer, StaticGraphTrait<K, VV, EV>> entry : traitMap.entrySet()) {
            CloseableIterator<K> iterator = entry.getValue().vertexIDIterator();
            iterators.add(this.mayScale ? shardFilter(iterator, entry.getKey(), k -> k) : iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    @Override
    public CloseableIterator<K> vertexIDIterator(IStatePushDown pushdown) {
        List<CloseableIterator<K>> iterators = new ArrayList<>();
        KeyGroup shardGroup = getShardGroup(pushdown);

        for (int shard = shardGroup.getStartKeyGroup(); shard <= shardGroup.getEndKeyGroup(); shard++) {
            StaticGraphTrait<K, VV, EV> trait = traitMap.get(shard);
            CloseableIterator<K> iterator = trait.vertexIDIterator(pushdown);
            iterators.add(this.mayScale ? shardFilter(iterator, shard, k -> k) : iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(IStatePushDown pushdown) {
        return getIterator(IVertex::getId, pushdown, StaticGraphTrait::getVertexIterator);
    }

    @Override
    public CloseableIterator<IVertex<K, VV>> getVertexIterator(List<K> keys, IStatePushDown pushdown) {
        return getIterator(keys, pushdown, StaticGraphTrait::getVertexIterator);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(IStatePushDown pushdown) {
        return getIterator(IEdge::getSrcId, pushdown, StaticGraphTrait::getEdgeIterator);
    }

    @Override
    public CloseableIterator<IEdge<K, EV>> getEdgeIterator(List<K> keys, IStatePushDown pushdown) {
        return getIterator(keys, pushdown, StaticGraphTrait::getEdgeIterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(
        IStatePushDown pushdown) {
        return getIterator(OneDegreeGraph::getKey, pushdown, StaticGraphTrait::getOneDegreeGraphIterator);
    }

    @Override
    public CloseableIterator<OneDegreeGraph<K, VV, EV>> getOneDegreeGraphIterator(List<K> keys, IStatePushDown pushdown) {
        return getIterator(keys, pushdown, StaticGraphTrait::getOneDegreeGraphIterator);
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(
        IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return getIterator(Tuple::getF0, pushdown,
            (trait, pd) -> (CloseableIterator<Tuple<K, R>>) trait.getEdgeProjectIterator(pd));
    }

    @Override
    public <R> CloseableIterator<Tuple<K, R>> getEdgeProjectIterator(List<K> keys,
                                                                     IStatePushDown<K, IEdge<K, EV>, R> pushdown) {
        return getIterator(keys, pushdown,
            (trait, keys1, pushdown1) -> trait.getEdgeProjectIterator(keys1, pushdown1));
    }

    @Override
    public Map<K, Long> getAggResult(IStatePushDown pushdown) {
        Map<Integer, Map<K, Long>> map = new HashMap<>();
        KeyGroup shardGroup = getShardGroup(pushdown);

        for (int shard = shardGroup.getStartKeyGroup(); shard <= shardGroup.getEndKeyGroup(); shard++) {
            map.put(shard, traitMap.get(shard).getAggResult(pushdown));
        }

        Map<K, Long> res = new HashMap<>();
        for (Entry<Integer, Map<K, Long>> partRes : map.entrySet()) {
            int keyGroupId = partRes.getKey();
            for (Entry<K, Long> entry : partRes.getValue().entrySet()) {
                if (keyGroupId != assigner.assign(entry.getKey())) {
                    continue;
                }
                res.put(entry.getKey(), entry.getValue());
            }
        }
        return res;
    }

    @Override
    public Map<K, Long> getAggResult(List<K> keys, IStatePushDown pushdown) {
        Map<K, Long> res = new HashMap<>();
        Map<Integer, List<K>> keyGroupMap = getKeyGroupMap(keys);
        for (Entry<Integer, List<K>> entry : keyGroupMap.entrySet()) {
            Preconditions.checkArgument(entry.getKey() >= this.shardGroup.getStartKeyGroup()
                && entry.getKey() <= this.shardGroup.getEndKeyGroup());

            res.putAll(getTraitById(entry.getKey()).getAggResult(entry.getValue(), pushdown));
        }
        return res;
    }

    private <R> CloseableIterator<R> getIterator(List<K> keys,
                                                 IStatePushDown pushdown,
                                                 TriFunction<StaticGraphTrait<K, VV, EV>, List<K>, IStatePushDown, CloseableIterator<R>> function) {
        List<CloseableIterator<R>> iterators = new ArrayList<>();
        Map<Integer, List<K>> keyGroupMap = getKeyGroupMap(keys);
        for (Entry<Integer, List<K>> entry : keyGroupMap.entrySet()) {
            Preconditions.checkArgument(entry.getKey() >= this.shardGroup.getStartKeyGroup()
                && entry.getKey() <= this.shardGroup.getEndKeyGroup());

            CloseableIterator<R> iterator = function.apply(getTraitById(entry.getKey()), entry.getValue(), pushdown);
            iterators.add(iterator);
        }
        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    private <R> CloseableIterator<R> getIterator(
        Function<R, K> keyExtractor,
        IStatePushDown pushdown,
        BiFunction<StaticGraphTrait<K, VV, EV>, IStatePushDown, CloseableIterator<R>> function) {
        List<CloseableIterator<R>> iterators = new ArrayList<>();

        KeyGroup shardGroup = getShardGroup(pushdown);

        int startShard = shardGroup.getStartKeyGroup();
        int endShard = shardGroup.getEndKeyGroup();

        for (int shard = startShard; shard <= endShard; shard++) {
            StaticGraphTrait<K, VV, EV> trait = traitMap.get(shard);
            CloseableIterator<R> iterator = function.apply(trait, pushdown);
            iterators.add(this.mayScale ? shardFilter(iterator, shard, keyExtractor) : iterator);
        }

        return iterators.size() == 1 ? iterators.get(0) : new MultiIterator<>(iterators.iterator());
    }

    private <T> CloseableIterator<T> shardFilter(CloseableIterator<T> iterator, int keyGroupId,
                                                 Function<T, K> keyExtractor) {
        return new IteratorWithFilter<>(iterator, t -> assigner.assign(keyExtractor.apply(t)) == keyGroupId);
    }

    private Map<Integer, List<K>> getKeyGroupMap(Collection<K> keySet) {
        return keySet.stream().collect(Collectors.groupingBy(c -> assigner.assign(c)));
    }
}
