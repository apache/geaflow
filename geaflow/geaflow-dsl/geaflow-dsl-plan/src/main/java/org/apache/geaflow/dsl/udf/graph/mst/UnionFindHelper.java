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
 * Union-Find数据结构辅助类.
 * 用于管理不相交集合的合并和查找操作.
 * 
 * <p>支持的操作：
 * - makeSet: 创建新集合
 * - find: 查找元素所属集合
 * - union: 合并两个集合
 * - getSetCount: 获取集合数量
 * - clear: 清空所有集合
 * 
 * @author TuGraph Analytics Team
 */
public class UnionFindHelper implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** 父节点映射. */
    private Map<Object, Object> parent;
    
    /** 秩映射（用于路径压缩优化）. */
    private Map<Object, Integer> rank;
    
    /** 集合大小映射. */
    private Map<Object, Integer> size;
    
    /** 集合数量. */
    private int setCount;

    /**
     * 构造函数.
     */
    public UnionFindHelper() {
        this.parent = new HashMap<>();
        this.rank = new HashMap<>();
        this.size = new HashMap<>();
        this.setCount = 0;
    }

    /**
     * 创建新集合.
     * @param x 元素
     */
    public void makeSet(Object x) {
        if (!parent.containsKey(x)) {
            parent.put(x, x);
            rank.put(x, 0);
            size.put(x, 1);
            setCount++;
        }
    }

    /**
     * 查找元素所属集合的根节点.
     * @param x 元素
     * @return 根节点
     */
    public Object find(Object x) {
        if (!parent.containsKey(x)) {
            return null;
        }
        
        if (!parent.get(x).equals(x)) {
            parent.put(x, find(parent.get(x)));
        }
        return parent.get(x);
    }

    /**
     * 合并两个集合.
     * @param x 第一个元素
     * @param y 第二个元素
     * @return 是否成功合并
     */
    public boolean union(Object x, Object y) {
        Object rootX = find(x);
        Object rootY = find(y);
        
        if (rootX == null || rootY == null) {
            return false;
        }
        
        if (rootX.equals(rootY)) {
            return false; // 已经在同一个集合中
        }
        
        // 按秩合并
        if (rank.get(rootX) < rank.get(rootY)) {
            Object temp = rootX;
            rootX = rootY;
            rootY = temp;
        }
        
        parent.put(rootY, rootX);
        size.put(rootX, size.get(rootX) + size.get(rootY));
        
        if (rank.get(rootX).equals(rank.get(rootY))) {
            rank.put(rootX, rank.get(rootX) + 1);
        }
        
        setCount--;
        return true;
    }

    /**
     * 获取集合数量.
     * @return 集合数量
     */
    public int getSetCount() {
        return setCount;
    }

    /**
     * 获取指定集合的大小.
     * @param x 集合中的任意元素
     * @return 集合大小
     */
    public int getSetSize(Object x) {
        Object root = find(x);
        if (root == null) {
            return 0;
        }
        return size.get(root);
    }

    /**
     * 检查两个元素是否在同一集合中.
     * @param x 第一个元素
     * @param y 第二个元素
     * @return 是否在同一集合中
     */
    public boolean isConnected(Object x, Object y) {
        Object rootX = find(x);
        Object rootY = find(y);
        return rootX != null && rootX.equals(rootY);
    }

    /**
     * 清空所有集合.
     */
    public void clear() {
        parent.clear();
        rank.clear();
        size.clear();
        setCount = 0;
    }

    /**
     * 检查Union-Find结构是否为空.
     * @return 是否为空
     */
    public boolean isEmpty() {
        return parent.isEmpty();
    }

    /**
     * 获取Union-Find结构中的元素数量.
     * @return 元素数量
     */
    public int size() {
        return parent.size();
    }

    /**
     * 检查元素是否存在.
     * @param x 元素
     * @return 是否存在
     */
    public boolean contains(Object x) {
        return parent.containsKey(x);
    }

    /**
     * 移除元素（及其所在集合）.
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
     * 获取指定集合的所有元素.
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
        return "UnionFindHelper{"
                + "parent=" + parent
                + ", rank=" + rank
                + ", size=" + size
                + ", setCount=" + setCount
                + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnionFindHelper that = (UnionFindHelper) obj;
        return setCount == that.setCount
            && Objects.equals(parent, that.parent)
            && Objects.equals(rank, that.rank)
            && Objects.equals(size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parent, rank, size, setCount);
    }
} 