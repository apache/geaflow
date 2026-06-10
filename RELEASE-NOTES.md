# Apache GeaFlow (Incubating) Release Notes

## 0.8.0

> Release date: 2026-06-02
> Release manager: Litao Lin (ltlin)

### Highlights

- **AI & Graph Intelligence**: Added vector store support, Lucene/Embedding-based search operators, graph consolidate algorithm, and CASTS for GeaFlow reasoning ability.
- **Graph Algorithms**: Added Louvain community detection, label propagation (LPA), connected components (CC), cluster coefficient, and Jaccard similarity algorithms.
- **ISO-GQL Compliance**: Implemented SAME predicate, PROPERTY_EXISTS predicate, and source/target predicate functions per ISO-GQL specification.
- **New Connectors**: Added Neo4j and Elasticsearch connectors; ODPS connector now supports dynamic partition writes; Paimon stream source support.

### Breaking Changes

- **MySQL JDBC driver is no longer bundled in convenience binaries.**
  The MySQL Connector/J dependency (`mysql:mysql-connector-java`) is licensed
  under GPLv2 with the FOSS / Classpath Exception, which is a
  [Category-X](https://www.apache.org/legal/resolved.html#category-x) license
  under the Apache Software Foundation policy and may not be redistributed
  as part of an Apache release.
  Starting from 0.8.0, the driver is declared with `<scope>provided</scope>`
  in `geaflow-store-jdbc` and `geaflow-console-common-dal`.
  **Action required for users:** when deploying `geaflow-console` or using
  the JDBC store with a MySQL backend, download a compatible MySQL JDBC
  driver and add it to the runtime classpath (e.g. drop the jar into
  Spring Boot's `--loader.path` or place it in the operator's `lib/`).

### Build Requirements

- **JDK 8 or JDK 11+** are both supported. The `jdk8` / `jdk11` Maven
  profiles activate automatically based on the JVM in use.
- **`geaflow-store-vector`** depends on Apache Lucene 9.x, which requires
  Java 11+. It is therefore included in the build **only when running on
  JDK 11+** (via the `jdk11-store-vector` profile in
  `geaflow/geaflow-plugins/geaflow-store/pom.xml`). Building with JDK 8
  simply omits this module — no `-pl` exclude flag is required.
- A standard `mvn clean install -DskipTests` from the source tarball root
  builds cleanly under both JDK 8 and JDK 11+.

### Removed from Source Release

- **`data/InferUDF.zip`** has been removed from the source repository because
  it contained pre-trained binary model weights (`model.pt`) and IDE/macOS
  metadata, which are not source-form artifacts permitted in an Apache
  source release. The reference inference UDF project can be obtained from
  the project download page or built by following the directory structure
  documented in `docs/docs-{cn,en}/source/3.quick_start/3.quick_start_infer&UDF.md`.

### Not Included in This Release

- **`geaflow-kubernetes-operator`** follows an independent release cadence
  (currently `0.4.0-SNAPSHOT`) and is excluded from the GeaFlow 0.8.0 source
  release tarball via `.gitattributes` `export-ignore`. Refer to the
  operator project's own release notes for its versioning.

### Features

- Support vector store (`geaflow-store-vector`) with Lucene-based indexing (#637)
- Add Lucene & Embedding-based search operators for lightweight context memory (#716)
- Add CASTS for GeaFlow reasoning ability (#737)
- Support graph consolidate algorithm (#729)
- Support Louvain community detection algorithm (#689)
- Implement label propagation (LPA) and connected components (CC) algorithms (#670)
- Support cluster coefficient algorithm (#640)
- Add Jaccard similarity algorithm (#650)
- Implement ISO-GQL SAME predicate for element identity comparison (#692)
- Add ISO-GQL PROPERTY_EXISTS predicate (#702)
- Implement ISO-GQL source/target predicate functions (#675)
- Add Neo4j and Elasticsearch connectors (#653)
- Write data into ODPS with dynamic partition support (#666)
- Support Paimon stream source (#662)
- Add MCP (Model Context Protocol) module (`geaflow-mcp`)
- Add AI module (`geaflow-ai`)

### Improvements

- Define BYTES_PER_KB constant for consistent byte-to-kilobyte conversions in metrics (#770)
- Extract varint constants to improve encoder readability (#744)
- Hard-coded optimization for cluster constants and magic numbers (#674)
- Use classifier for artifact resolution (#720)
- Add try-with-resources block around ProcessLoggerManager (#687)
- Standardize editor configs for cross-platform development (#648)
- Extract vertex/edge projector rules (#630)
- Update repository references from tugraph-family to apache (#746)

### Bug Fixes

- Fix data loss after failover (#633)
- Add check for empty search results to prevent array out of bounds (#769)
- Add runtime checks for graph accessors in multiple classes (#774)
- Exclude lucene-core from elasticsearch client dependency and add specific version (#778)
- Handle null and empty inputs in average methods (#760)
- Fix raw type usage: replace Collections.EMPTY_LIST with Collections.emptyList() (#766)
- Update RocksdbClient to use getDeclaredConstructor for options instantiation (#749)
- Improve error handling in searchVectorIndex method (#750)
- Correct typos in log messages and comments (#752)
- Fix memory management in mmap_ipc.cpp (#725)

### Dependency Upgrades

- Bump lz4-java from 1.3.0 to 1.10.1 (#714, #715)
- Bump maven-gpg-plugin to 3.2.7
- Bump maven-compiler-plugin to 3.11.0
- Add nexus-staging-maven-plugin 1.7.0

### Contributors

- Appointat
- Haodong Tang
- Jason Yao
- Leomrlin
- Loognqiang
- Qingwen Zhao
- SeasonPilot
- Tengting Xu
- Wang Rui
- Weichen Zhao
- accevolve
- chzhoo
- hey-money
- kitalkuyo-gita
- moses
- shown
- vamossagar12
- yazong
- 明城
