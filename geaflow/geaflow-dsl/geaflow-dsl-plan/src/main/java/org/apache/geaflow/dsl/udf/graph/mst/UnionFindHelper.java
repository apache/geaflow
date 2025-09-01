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

package org.apache.geaflow.dsl.udf.graph.mst;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Union-Find辅助类
 * 用于维护图的连通性检测，支持路径压缩和按秩合并优化
 * 
 * 功能：
 * - 创建集合
 * - 查找元素所属集合
 * - 合并两个集合
 * - 检测两个元素是否连通
 * 
 * @author TuGraph Analytics Team
 */
public class UnionFindHelper implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** 父节点映射 */
    private Map<Object, Object> parent;
    
    /** 秩映射（用于按秩合并优化） */
    private Map<Object, Integer> rank;
    
    /** 集合大小映射 */
    private Map<Object, Integer> size;
    
    /** 集合数量 */
    private int setCount;

    /**
     * 构造函数
     */
    public UnionFindHelper() {
        this.parent = new HashMap<>();
        this.rank = new HashMap<>();
        this.size = new HashMap<>();
        this.setCount = 0;
    }

    /**
     * 创建新的集合
     * @param x 元素
     */
    public void makeSet(Object x) {
        if (!parent.containsKey(x)) {
            parent.put(x, x); // 自己为父节点
            rank.put(x, 0);   // 初始秩为0
            size.put(x, 1);   // 初始大小为1
            setCount++;       // 集合数量加1
        }
    }

    /**
     * 查找元素所属集合的根节点
     * 使用路径压缩优化
     * @param x 元素
     * @return 根节点
     */
    public Object find(Object x) {
        if (!parent.containsKey(x)) {
            makeSet(x);
        }
        
        // 路径压缩：将查找路径上的所有节点直接连接到根节点
        if (!parent.get(x).equals(x)) {
            parent.put(x, find(parent.get(x)));
        }
        return parent.get(x);
    }

    /**
     * 合并两个集合
     * 使用按秩合并优化
     * @param x 元素1
     * @param y 元素2
     * @return 是否成功合并（如果已经在同一个集合中返回false）
     */
    public boolean union(Object x, Object y) {
        Object rootX = find(x);
        Object rootY = find(y);

        if (rootX.equals(rootY)) {
            return false; // 已经在同一个集合中
        }

        // 按秩合并：将秩较小的树连接到秩较大的树
        int rankX = rank.get(rootX);
        int rankY = rank.get(rootY);

        if (rankX < rankY) {
            // 将rootX连接到rootY
            parent.put(rootX, rootY);
            size.put(rootY, size.get(rootY) + size.get(rootX));
        } else if (rankX > rankY) {
            // 将rootY连接到rootX
            parent.put(rootY, rootX);
            size.put(rootX, size.get(rootX) + size.get(rootY));
        } else {
            // 秩相等时，将rootY连接到rootX，并增加rootX的秩
            parent.put(rootY, rootX);
            rank.put(rootX, rankX + 1);
            size.put(rootX, size.get(rootX) + size.get(rootY));
        }

        setCount--; // 集合数量减1
        return true;
    }

    /**
     * 检测两个元素是否连通
     * @param x 元素1
     * @param y 元素2
     * @return 是否连通
     */
    public boolean connected(Object x, Object y) {
        return find(x).equals(find(y));
    }

    /**
     * 获取集合的数量
     * @return 集合数量
     */
    public int getSetCount() {
        return setCount;
    }

    /**
     * 获取指定元素所在集合的大小
     * @param x 元素
     * @return 集合大小
     */
    public int getSetSize(Object x) {
        Object root = find(x);
        return size.get(root);
    }

    /**
     * 获取所有集合的根节点
     * @return 根节点集合
     */
    public java.util.Set<Object> getRoots() {
        java.util.Set<Object> roots = new java.util.HashSet<>();
        for (Object x : parent.keySet()) {
            roots.add(find(x));
        }
        return roots;
    }

    /**
     * 重置Union-Find结构
     */
    public void reset() {
        parent.clear();
        rank.clear();
        size.clear();
        setCount = 0;
    }

    /**
     * 检查Union-Find结构是否为空
     * @return 是否为空
     */
    public boolean isEmpty() {
        return parent.isEmpty();
    }

    /**
     * 获取Union-Find结构中的元素数量
     * @return 元素数量
     */
    public int size() {
        return parent.size();
    }

    /**
     * 检查元素是否存在
     * @param x 元素
     * @return 是否存在
     */
    public boolean contains(Object x) {
        return parent.containsKey(x);
    }

    /**
     * 移除元素（及其所在集合）
     * @param x 元素
     * @return 是否成功移除
     */
    public boolean remove(Object x) {
        if (!parent.containsKey(x)) {
            return false;
        }

        Object root = find(x);
        int rootSize = size.get(root);
        
        if (rootSize == 1) {
            // 如果集合只有一个元素，直接移除
            parent.remove(x);
            rank.remove(x);
            size.remove(x);
            setCount--;
        } else {
            // 如果集合有多个元素，需要重新组织
            // 这里简化处理，实际应用中可能需要更复杂的逻辑
            parent.remove(x);
            size.put(root, rootSize - 1);
        }
        
        return true;
    }

    /**
     * 获取指定集合的所有元素
     * @param root 集合根节点
     * @return 集合中的所有元素
     */
    public java.util.Set<Object> getSetElements(Object root) {
        java.util.Set<Object> elements = new java.util.HashSet<>();
        for (Object x : parent.keySet()) {
            if (find(x).equals(root)) {
                elements.add(x);
            }
        }
        return elements;
    }

    @Override
    public String toString() {
        return "UnionFindHelper{" +
                "parent=" + parent +
                ", rank=" + rank +
                ", size=" + size +
                ", setCount=" + setCount +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UnionFindHelper that = (UnionFindHelper) obj;
        return setCount == that.setCount &&
               Objects.equals(parent, that.parent) &&
               Objects.equals(rank, that.rank) &&
               Objects.equals(size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parent, rank, size, setCount);
    }
} 