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

package org.apache.geaflow.state.partition;

import java.util.List;
import java.util.Set;
import org.apache.geaflow.state.pushdown.filter.EdgeLabelFilter;
import org.apache.geaflow.state.pushdown.filter.VertexLabelFilter;
import org.apache.geaflow.state.pushdown.filter.inner.GraphFilter;
import org.apache.geaflow.state.pushdown.filter.inner.IGraphFilter;
import org.junit.Assert;
import org.junit.Test;

public class DefaultPartitionManagerTest {

    @Test
    public void testParseLabelsFromFilter() {
        DefaultPartitionManager manager = new DefaultPartitionManager();
        
        // Register some test partitions
        manager.registerPartition("partition_person", 
            new PartitionMetadata("partition_person", "person", 1000, System.currentTimeMillis(), true));
        manager.registerPartition("partition_trade", 
            new PartitionMetadata("partition_trade", "trade", 2000, System.currentTimeMillis(), true));
        manager.registerPartition("partition_relation", 
            new PartitionMetadata("partition_relation", "relation", 1500, System.currentTimeMillis(), true));
        
        // Test with vertex label filter
        IGraphFilter vertexFilter = GraphFilter.of(new VertexLabelFilter("person", "trade"));
        List<String> partitions = manager.parseLabelsFromFilter(vertexFilter);
        Assert.assertEquals(2, partitions.size());
        Assert.assertTrue(partitions.contains("partition_person"));
        Assert.assertTrue(partitions.contains("partition_trade"));
        
        // Test with edge label filter
        IGraphFilter edgeFilter = GraphFilter.of(new EdgeLabelFilter("relation"));
        partitions = manager.parseLabelsFromFilter(edgeFilter);
        Assert.assertEquals(1, partitions.size());
        Assert.assertTrue(partitions.contains("partition_relation"));
        
        // Test with null filter (should return all partitions)
        partitions = manager.parseLabelsFromFilter(null);
        Assert.assertEquals(3, partitions.size());
        
        // Test with non-existent labels
        IGraphFilter nonExistentFilter = GraphFilter.of(new VertexLabelFilter("nonexistent"));
        partitions = manager.parseLabelsFromFilter(nonExistentFilter);
        Assert.assertEquals(3, partitions.size()); // Should return all partitions
    }

    @Test
    public void testPartitionManagement() {
        DefaultPartitionManager manager = new DefaultPartitionManager();
        
        // Test partition registration
        PartitionMetadata metadata = new PartitionMetadata("test_partition", "test_label", 100, System.currentTimeMillis(), true);
        manager.registerPartition("test_partition", metadata);
        
        Assert.assertTrue(manager.partitionExists("test_partition"));
        Assert.assertEquals(metadata, manager.getPartitionMetadata("test_partition"));
        
        // Test getAllPartitions
        Set<String> allPartitions = manager.getAllPartitions();
        Assert.assertEquals(1, allPartitions.size());
        Assert.assertTrue(allPartitions.contains("test_partition"));
        
        // Test partition unregistration
        manager.unregisterPartition("test_partition");
        Assert.assertFalse(manager.partitionExists("test_partition"));
        Assert.assertNull(manager.getPartitionMetadata("test_partition"));
        Assert.assertEquals(0, manager.getAllPartitions().size());
    }
}
