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
 * Union-Find data structure helper class.
 * Used for managing union and find operations on disjoint sets.
 * 
 * <p>Supported operations:
 * - makeSet: Create new set
 * - find: Find the set an element belongs to
 * - union: Merge two sets
 * - getSetCount: Get number of sets
 * - clear: Clear all sets
 * 
 * @author Geaflow Team
 */
public class UnionFindHelper implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /** Parent node mapping. */
    private Map<Object, Object> parent;
    
    /** Rank mapping (for path compression optimization). */
    private Map<Object, Integer> rank;
    
    /** Set size mapping. */
    private Map<Object, Integer> size;
    
    /** Number of sets. */
    private int setCount;

    /**
     * Constructor.
     */
    public UnionFindHelper() {
        this.parent = new HashMap<>();
        this.rank = new HashMap<>();
        this.size = new HashMap<>();
        this.setCount = 0;
    }

    /**
     * Create new set.
     * @param x Element
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
     * Find the root node of the set an element belongs to.
     * @param x Element
     * @return Root node
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
     * Merge two sets.
     * @param x First element
     * @param y Second element
     * @return Whether merge was successful
     */
    public boolean union(Object x, Object y) {
        Object rootX = find(x);
        Object rootY = find(y);
        
        if (rootX == null || rootY == null) {
            return false;
        }
        
        if (rootX.equals(rootY)) {
            return false; // Already in the same set
        }
        
        // Union by rank
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
     * Get number of sets.
     * @return Number of sets
     */
    public int getSetCount() {
        return setCount;
    }

    /**
     * Get size of specified set.
     * @param x Any element in the set
     * @return Set size
     */
    public int getSetSize(Object x) {
        Object root = find(x);
        if (root == null) {
            return 0;
        }
        return size.get(root);
    }

    /**
     * Check if two elements are in the same set.
     * @param x First element
     * @param y Second element
     * @return Whether they are in the same set
     */
    public boolean isConnected(Object x, Object y) {
        Object rootX = find(x);
        Object rootY = find(y);
        return rootX != null && rootX.equals(rootY);
    }

    /**
     * Clear all sets.
     */
    public void clear() {
        parent.clear();
        rank.clear();
        size.clear();
        setCount = 0;
    }

    /**
     * Check if Union-Find structure is empty.
     * @return Whether it is empty
     */
    public boolean isEmpty() {
        return parent.isEmpty();
    }

    /**
     * Get number of elements in Union-Find structure.
     * @return Number of elements
     */
    public int size() {
        return parent.size();
    }

    /**
     * Check if element exists.
     * @param x Element
     * @return Whether it exists
     */
    public boolean contains(Object x) {
        return parent.containsKey(x);
    }

    /**
     * Remove element (and its set).
     * @param x Element
     * @return Whether removal was successful
     */
    public boolean remove(Object x) {
        if (!parent.containsKey(x)) {
            return false;
        }

        Object root = find(x);
        int rootSize = size.get(root);
        
        if (rootSize == 1) {
            // If set has only one element, remove directly
            parent.remove(x);
            rank.remove(x);
            size.remove(x);
            setCount--;
        } else {
            // If set has multiple elements, need to reorganize
            // Simplified handling here, actual applications may need more complex logic
            parent.remove(x);
            size.put(root, rootSize - 1);
        }
        
        return true;
    }

    /**
     * Get all elements in specified set.
     * @param root Set root node
     * @return All elements in the set
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