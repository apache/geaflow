# Migrating from RocksDB to LMDB

This guide provides step-by-step instructions for migrating GeaFlow applications from RocksDB storage backend to LMDB.

## Overview

Migrating from RocksDB to LMDB involves configuration changes and optional data migration. LMDB offers better read performance and lower memory usage, making it ideal for read-heavy workloads.

## Quick Decision Guide

**Migrate to LMDB if**:
- Read operations dominate your workload (>70%)
- You want predictable latency (no compaction spikes)
- Memory usage is a concern
- You prefer simpler operational model

**Stay with RocksDB if**:
- Write operations dominate your workload (>60%)
- You need dynamic storage growth
- Data size is unpredictable
- You frequently update existing keys

## Migration Approaches

### Approach 1: Clean Start (Recommended)

Start with a fresh LMDB database. Suitable when you can rebuild state or start from scratch.

**Pros**: Simple, no data conversion needed
**Cons**: Requires state rebuild

**Steps**:

1. **Backup existing RocksDB data** (optional):
```bash
# Archive current RocksDB checkpoints
tar -czf rocksdb_backup.tar.gz /path/to/rocksdb/checkpoints/
```

2. **Update configuration**:
```properties
# Before
geaflow.store.type=ROCKSDB

# After
geaflow.store.type=LMDB
geaflow.store.lmdb.map.size=107374182400  # Adjust based on data size
geaflow.store.lmdb.max.readers=126
geaflow.store.lmdb.sync.mode=META_SYNC
```

3. **Clear old data directories**:
```bash
rm -rf /path/to/job/work/directory/*
```

4. **Restart application**:
```bash
# Application will use LMDB from this point
./start_geaflow_job.sh
```

### Approach 2: Data Export/Import

Export data from RocksDB and import into LMDB. Suitable for preserving existing state.

**Pros**: Preserves existing data
**Cons**: Requires downtime, custom export/import logic

**Steps**:

1. **Create export utility**:
```java
public class RocksDBExporter {
    public static void exportToFile(IKVStatefulStore<K, V> rocksdbStore,
                                    String outputFile) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            // Export all key-value pairs
            try (CloseableIterator<Tuple<K, V>> iter = rocksdbStore.getKeyValueIterator()) {
                while (iter.hasNext()) {
                    Tuple<K, V> entry = iter.next();
                    // Serialize to JSON or custom format
                    writer.write(serialize(entry));
                    writer.newLine();
                }
            }
        }
    }
}
```

2. **Export RocksDB data**:
```java
// Using RocksDB backend
Configuration rocksdbConfig = new Configuration();
rocksdbConfig.put(StateConfigKeys.STATE_BACKEND_TYPE.getKey(), "ROCKSDB");

IStoreBuilder rocksdbBuilder = StoreBuilderFactory.build(StoreType.ROCKSDB.name());
IKVStatefulStore<String, String> rocksdbStore =
    rocksdbBuilder.getStore(DataModel.KV, rocksdbConfig);

RocksDBExporter.exportToFile(rocksdbStore, "data_export.txt");
```

3. **Import to LMDB**:
```java
// Using LMDB backend
Configuration lmdbConfig = new Configuration();
lmdbConfig.put(StateConfigKeys.STATE_BACKEND_TYPE.getKey(), "LMDB");
lmdbConfig.put(LmdbConfigKeys.LMDB_MAP_SIZE.getKey(), "107374182400");

IStoreBuilder lmdbBuilder = StoreBuilderFactory.build(StoreType.LMDB.name());
IKVStatefulStore<String, String> lmdbStore =
    lmdbBuilder.getStore(DataModel.KV, lmdbConfig);

// Import data
try (BufferedReader reader = new BufferedReader(new FileReader("data_export.txt"))) {
    String line;
    int batchSize = 0;
    while ((line = reader.readLine()) != null) {
        Tuple<String, String> entry = deserialize(line);
        lmdbStore.put(entry.f0, entry.f1);

        // Flush every 10000 records
        if (++batchSize >= 10000) {
            lmdbStore.flush();
            batchSize = 0;
        }
    }
    lmdbStore.flush();
}
```

### Approach 3: Parallel Operation

Run both backends in parallel during transition period. Suitable for gradual migration.

**Pros**: Zero downtime, gradual rollout
**Cons**: Complex setup, requires dual writes

**Steps**:

1. **Implement dual-write logic**:
```java
public class DualBackendStore<K, V> implements IKVStatefulStore<K, V> {
    private final IKVStatefulStore<K, V> rocksdbStore;
    private final IKVStatefulStore<K, V> lmdbStore;
    private final boolean readFromLmdb;

    @Override
    public void put(K key, V value) {
        rocksdbStore.put(key, value);
        lmdbStore.put(key, value);
    }

    @Override
    public V get(K key) {
        return readFromLmdb ? lmdbStore.get(key) : rocksdbStore.get(key);
    }
}
```

2. **Configure both backends**:
```properties
# Primary backend
geaflow.store.type=ROCKSDB

# Secondary backend for testing
geaflow.store.lmdb.enabled=true
geaflow.store.lmdb.map.size=107374182400
```

3. **Gradual switchover**:
- Week 1: Write to both, read from RocksDB
- Week 2: Write to both, read from LMDB (validate)
- Week 3: Write to LMDB only, remove RocksDB

## Configuration Mapping

### Basic Configuration

| RocksDB Config | LMDB Equivalent | Notes |
|----------------|-----------------|-------|
| `geaflow.store.rocksdb.write.buffer.size` | Not applicable | LMDB doesn't use write buffers |
| `geaflow.store.rocksdb.max.write.buffer.number` | Not applicable | Single write transaction |
| `geaflow.store.rocksdb.block.size` | Not applicable | B+tree structure |
| `geaflow.store.rocksdb.cache.size` | Not applicable | Uses OS page cache |
| `geaflow.store.rocksdb.compaction.style` | Not applicable | No compaction needed |

### New LMDB Settings

```properties
# Map size (REQUIRED - estimate based on RocksDB data size * 2)
geaflow.store.lmdb.map.size=107374182400

# Max concurrent readers
geaflow.store.lmdb.max.readers=126

# Sync mode (similar to RocksDB WAL sync)
geaflow.store.lmdb.sync.mode=META_SYNC
```

### Checkpoint Configuration

Both backends use the same checkpoint configuration:

```properties
# No changes needed
geaflow.file.persistent.type=LOCAL
geaflow.file.persistent.root=/path/to/checkpoints
```

## Estimating Map Size

LMDB requires pre-allocating map size. Use these guidelines:

### Method 1: Based on RocksDB Size

```bash
# Check current RocksDB database size
du -sh /path/to/rocksdb/database/

# Set LMDB map size to 2x RocksDB size
# Example: If RocksDB is 45GB, set map size to 100GB
geaflow.store.lmdb.map.size=107374182400
```

### Method 2: Based on Data Characteristics

```properties
# Formula: (# records × average_value_size × 1.5)
# Example: 10M records × 2KB/record × 1.5 = 30GB
geaflow.store.lmdb.map.size=32212254720  # 30GB
```

### Method 3: Conservative Estimate

```properties
# Small: < 50M records
geaflow.store.lmdb.map.size=21474836480  # 20GB

# Medium: 50M - 500M records
geaflow.store.lmdb.map.size=107374182400  # 100GB

# Large: > 500M records
geaflow.store.lmdb.map.size=536870912000  # 500GB
```

## Performance Comparison

After migration, expect these performance changes:

### Read Performance

```
Random Reads:  30-50% faster
Sequential Reads: 40-60% faster
Range Scans: 20-40% faster
```

### Write Performance

```
Random Writes: 10-20% slower
Sequential Writes: 5-15% slower
Batch Writes: Similar performance
```

### Memory Usage

```
Peak Memory: 60-80% reduction
Steady State: 50-70% reduction
```

### Latency Stability

```
P99 Latency: 40-60% improvement (no compaction spikes)
P999 Latency: 50-70% improvement
```

## Validation Steps

After migration, validate LMDB backend is working correctly:

### 1. Functional Validation

```java
@Test
public void validateMigration() {
    // Test basic operations
    kvStore.put("key1", "value1");
    kvStore.flush();
    assertEquals("value1", kvStore.get("key1"));

    // Test checkpoint/recovery
    kvStore.archive(1);
    kvStore.drop();
    kvStore.recovery(1);
    assertEquals("value1", kvStore.get("key1"));
}
```

### 2. Performance Validation

```bash
# Run benchmark comparing RocksDB vs LMDB
./run_benchmark.sh --backend=LMDB --duration=300

# Compare results
# - Read throughput should be higher
# - Write throughput may be slightly lower
# - Latency should be more stable
```

### 3. Memory Validation

```bash
# Monitor memory usage over time
top -p $(pgrep -f geaflow) -b -d 60 > memory_usage.log

# Memory should stabilize lower than RocksDB
```

## Rollback Plan

If issues arise, rollback to RocksDB:

### Quick Rollback

```properties
# Revert configuration
geaflow.store.type=ROCKSDB

# Restore from RocksDB checkpoint (if preserved)
# Application restarts with RocksDB
```

### Data Recovery

```bash
# If you backed up RocksDB data
tar -xzf rocksdb_backup.tar.gz -C /path/to/restore/

# Update configuration
geaflow.store.type=ROCKSDB
geaflow.file.persistent.root=/path/to/restore/

# Restart application
./start_geaflow_job.sh
```

## Common Issues and Solutions

### Issue 1: Map Size Too Small

**Symptom**: `MDB_MAP_FULL` error

**Solution**:
```properties
# Increase map size (requires restart)
geaflow.store.lmdb.map.size=214748364800  # Double it
```

### Issue 2: Slower Write Performance

**Symptom**: Writes are slower than RocksDB

**Solution**:
```properties
# Reduce sync frequency (less safe)
geaflow.store.lmdb.sync.mode=NO_SYNC

# Or batch writes more aggressively
# (Application level - increase batch size)
```

### Issue 3: High Memory Usage

**Symptom**: Memory usage higher than expected

**Solution**:
```properties
# Reduce max readers
geaflow.store.lmdb.max.readers=64

# Enable NO_TLS
geaflow.store.lmdb.no.tls=true
```

### Issue 4: Data Inconsistency

**Symptom**: Data doesn't match RocksDB

**Solution**:
```bash
# Verify export/import process
# Check serialization/deserialization
# Validate checksum of exported data
```

## Best Practices

### 1. Test in Staging First

```bash
# Always test migration in non-production environment
# Run for at least 1 week
# Monitor all metrics closely
```

### 2. Backup Before Migration

```bash
# Backup RocksDB checkpoints
tar -czf rocksdb_backup_$(date +%Y%m%d).tar.gz /checkpoints/

# Keep backups for at least 30 days
```

### 3. Monitor After Migration

```bash
# Key metrics to monitor:
# - Read/write throughput
# - Latency percentiles (p50, p95, p99)
# - Memory usage
# - Map size utilization
# - Error rates
```

### 4. Gradual Rollout

```bash
# Rollout plan:
# Day 1: Single test instance
# Day 3: 10% of instances
# Week 1: 50% of instances
# Week 2: 100% of instances
```

## Performance Tuning After Migration

### For Read-Heavy Workloads

```properties
# Optimize for reads
geaflow.store.lmdb.max.readers=256
geaflow.store.lmdb.no.tls=true
geaflow.store.lmdb.sync.mode=META_SYNC
```

### For Write-Heavy Workloads

```properties
# Optimize for writes (with caution)
geaflow.store.lmdb.sync.mode=NO_SYNC
geaflow.store.lmdb.write.map=true  # Linux only

# Consider staying with RocksDB if writes dominate
```

### For Balanced Workloads

```properties
# Balanced configuration
geaflow.store.lmdb.max.readers=126
geaflow.store.lmdb.sync.mode=META_SYNC
```

## Migration Checklist

- [ ] Backup RocksDB data and configuration
- [ ] Estimate LMDB map size requirements
- [ ] Update application configuration
- [ ] Test in development environment
- [ ] Test in staging environment
- [ ] Create rollback plan
- [ ] Schedule maintenance window (if needed)
- [ ] Execute migration
- [ ] Validate functionality
- [ ] Monitor performance metrics
- [ ] Monitor memory usage
- [ ] Document any issues encountered
- [ ] Keep RocksDB backups for 30 days
- [ ] Update operational documentation

## Support

For migration assistance:
- GitHub Issues: https://github.com/TuGraph-family/tugraph-analytics/issues
- Community Forum: Join our discussion forum
- Email: Contact the GeaFlow team

## References

- [LMDB Configuration Guide](README.md#configuration-reference)
- [Performance Tuning Guide](README.md#performance-tuning)
- [GeaFlow Documentation](https://tugraph-analytics.readthedocs.io/)
