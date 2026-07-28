# Feature：关键词检索索引常驻化与就地增量维护

> 模块：`geaflow-ai`　状态：已实现、已验证

---

## 1. 摘要

| 项 | 内容 |
|---|---|
| 能力 | 关键词检索使用按图常驻的 Lucene 索引；查询路径不做索引构建，写入以增量方式就地维护 |
| 手段 | 索引常驻化 + Lucene 主键 update / delete by term + 文本化结果记忆化 + 图版本号兜底 |
| 读效果 | 10000 顶点，每查询 **0.41 ~ 0.46 ms**（每查询重建方案 98.4 ~ 119.5 ms） |
| 写效果 | 5000 顶点、写查交替，每轮 **1.60 ~ 1.81 ms**（失效重建方案 44.5 ~ 47.1 ms）；全量构建次数 41 → **1** |
| 语义 | 召回结果与每查询重建方案一致，由等价性测试保证 |
| 规模 | 主干 17 个文件 +565/−57 行，新增 `ResidentSearchIndex` 256 行；新增测试 2 个文件 |
| 验证 | `mvn -pl geaflow-ai -am clean install` 全绿，17 个测试通过，Checkstyle 0 违规，RAT 通过 |

---

## 2. 要解决的问题

关键词检索入口是 `SessionOperator.apply()`，会话首轮（子图为空）走全局检索分支。原实现每次查询都从零构建索引：

```
scanVertex()                          遍历全图顶点
  └─ indexStore.getEntityIndex(v)     每个顶点做一次 verbalize
new GraphSearchStore()                新建 Lucene 内存索引（方法内局部变量）
  └─ indexVertex(...) × V             逐个 addDoc
close()                               关闭 writer 让文档对 reader 可见
search(query)                         真正的检索
                                      方法返回，索引被 GC
```

单次冷查询代价 `O(V × (verbalize + 分词 + 倒排写入))`，真正的检索只占其中极小一部分。四个叠加的代价点：

1. **索引每查询重建后丢弃**。`SearchStore` 用 `ByteBuffersDirectory`（纯内存），且是方法内局部变量。
2. **`close()` 被当作 flush 手段**，索引生命周期被固定为一次性。
3. **每查询把整张图重新文本化一遍**。`EntityAttributeIndexStore.getEntityIndex()` 在全图扫描中被调用 V 次，每次构造 `SubGraph`、渲染 prompt、分配大量临时字符串。
4. **多轮会话白跑一次全局检索**。`apply()` 无条件先调用全局检索，但子图非空的热路径不引用其返回值，结果被直接丢弃。

同时 `EmbeddingOperator` 有同类问题：为组装候选集遍历全图顶点，而没有 embedding 的顶点在打分阶段本就会被跳过，扫描是多余的。

---

## 3. 设计

### 3.1 目标与约束

| 目标 | 约束 |
|---|---|
| 查询路径上不做索引构建 | 召回结果与每查询重建方案一致 |
| 写入代价与变更量成正比，而非与图规模成正比 | 不修改 `SearchOperator` / `IndexStore` / `GraphAccessor` 既有方法签名 |
| 图变更后不返回脏数据 | 保持 Lucene 8.11.2 / JDK 8 兼容，不触碰 Lucene 9 + JDK 11 这个待决议约束 |

### 3.2 设计依据：成熟实现怎么做

倒排索引与向量索引处理「派生索引 + 可变主数据」都收敛到同一套结构：**索引分段且不可变 → 变更用增量表达（更新 = 标记删除 + 新增，删除只打标记）→ 后台异步合并回收**。没有产品用「整体失效 + 懒重建」。

- **Lucene / Elasticsearch**：文档被删或更新时只在段级位图标一位，检索跳过被标记的文档；NRT reader 重开耗时与上次重开以来的变更量成正比，而非与索引总量成正比。ES 在其上用 `refresh_interval`（默认 1 秒）攒批，并靠后台合并控制段数量。
- **关系库二级索引**：DML 在同一事务内维护索引项，没有「失效」概念；Postgres GIN 的 `fastupdate` 本质是攒批。
- **向量库**：Milvus 新写入进 growing segment，转 sealed 后异步建索引，索引就绪前退回原始向量暴力检索；HNSW 删不掉节点（会破坏图连通性），统一打墓碑 + 周期性 compaction。

**对照 hugegraph-ai**：它的查询路径同样零构建成本 —— 索引在算子构造期由 `FaissVectorIndex.from_name()` 从磁盘一次性加载，算子随 pipeline 被 `Scheduler` 的 `GPipelineManager` 池化复用，写入是独立的离线 flow（`BuildVectorIndexFlow` 等）。值得注意的是它用的是 `faiss.IndexFlatL2`，同样是暴力精确检索而非 ANN，所以优势不在算法层，而在四个工程决策：索引常驻、索引对象小（只索引顶点 ID 文本而非完整 verbalization）、先 Gremlin 精确匹配未命中才走向量、图扩展下推给图数据库。

反过来它也有短板：`node_init` 只在 pipeline 首次创建时执行，**没有任何失效机制** —— 离线重建索引后，池中已存在的 pipeline 仍持有旧快照。它能忍受这一点是因为定位是批式知识库（先建索引再问答）；而 `geaflow-ai` 的 GraphMemory 同进程同图可以边写边查，必须有失效机制，这部分没有现成实现可抄。

### 3.3 文档集严格等价

常驻索引收录的文档集 = **所有 `getEntityIndex()` 返回非空的顶点**，与每查询重建方案完全相同：

- 边被排除，与全局检索只 `scanVertex()` 的语义一致
- 全量构建时用 `Set<GraphEntity> indexedEntities` 去重，等价于原实现以 `Map` 键去重
- Lucene 查询串、`topN`（`Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN = 30`）、`StandardAnalyzer` 均不变

文档集、查询、打分器三者相同，因此召回相同。

### 3.4 写入以 Lucene 主键增量维护

Lucene 原生支持增量维护，前提是每个文档有一个可精确定位的 term。因此每个文档带一个不分词主键字段 `SearchConstants.KEY`，取值为 `ModelUtils.getGraphEntityKey(entity)`（顶点 `V{id}{label}`，边 `E{src}{label}{dst}`）—— 这个 key 生成逻辑本已存在，`EmbeddingIndexStore` 就用它做 jsonl 行键。

| 操作 | 实现 | 代价 |
|---|---|---|
| 全量构建 | `addDocument`（扫描保证每顶点只出现一次，无需去重开销） | O(V)，仅首次 |
| 新增 / 更新 | `updateDocument(new Term(KEY, key), doc)` | O(变更量) |
| 删除 | `deleteDocuments(new Term(KEY, key))`，只在段级位图打标记 | O(1) |
| 可见性 | 批次结束后一次 `refresh()`（`commit()` + `openIfChanged()`） | O(变更量) |

`updateDocument` 对「已存在」与「不存在」处理一致，因此 **`onEntitiesUpserted` 是幂等的，调用方不需要提供精确增量**，重复上报同一实体不会产生重复文档。这消除了「维护正确性依赖调用方给出准确 delta」的隐式契约。

服务层因此拆成三个语义明确的入口，而不是一个布尔开关：

```
GraphMemoryServer
  ├─ onEntitiesUpserted(entities)   新增与更新，就地 upsert
  ├─ onEntitiesRemoved(entities)    删除，就地 delete
  └─ onSchemaChanged()              无法按实体表达的变更，整体失效
```

只有 schema 变更走整体失效 —— 它会改变每一个实体的 verbalize 结果。

### 3.5 图版本号作为兜底

图可以被绕过服务层直接改写（例如经 `MemoryMutableGraph`），这类变更没有任何钩子会触发。因此由图自身维护版本号：

```
MemoryGraph
  ├─ version        任何变更 +1（点、边、schema）
  └─ vertexVersion  仅点与 schema 变更 +1

GraphAccessor
  ├─ getGraphVersion()    默认 VERSION_UNSUPPORTED (-1)
  └─ getVertexVersion()   默认委托 getGraphVersion()

VerbalizationFunction
  └─ getSourceVersion()   默认 VERSION_UNSUPPORTED；SubgraphSemanticPromptFunction 透传 accessor 版本
```

- `ResidentSearchIndex` 记录构建时的 `vertexVersion`；就地维护完成后把它推进到当前值，所以正常写入路径不触发重建
- 比对不一致说明发生了未经通知的改图 → 整体重建，而不是返回脏数据
- `EntityAttributeIndexStore` 的文本化缓存比对 `getSourceVersion()`，不一致即整体清空

**版本不可用时自动降级**：返回 `VERSION_UNSUPPORTED` 的 accessor（如 `EmptyGraphAccessor`，及任何未实现版本上报的实现）使常驻索引每次重建、缓存完全不启用 —— 退化为每查询重建行为。

`MemoryGraph` 中失败的变更同样 bump 版本：过度失效只是慢，漏失效是正确性缺陷。

**`vertexVersion` 与 `version` 分离**：常驻索引的文档只由顶点决定（顶点的 verbalize 只读该顶点自身），所以它 watch `vertexVersion`，边写入不会使其失效 —— 这在 consolidate 场景下有实际意义，一次插入会带来约 30 次 `addEdge`。文本化缓存 watch 全局 `version`，因为边的 verbalize 会读取两端顶点（`schema.getPrompt(edge, start, end)`），依赖面更广。

### 3.6 更新与失效时机

全部懒执行，没有定时任务，也没有后台重建线程。

| 时机 | 行为 |
|---|---|
| 首次冷查询 | `ensureGlobalIndex()` 全量构建一次 |
| 后续冷查询，`vertexVersion` 未变 | 直接复用，零构建开销 |
| 后续冷查询，`vertexVersion` 已变 | 整体重建 |
| `/graph/insertEntity`（新增或更新），索引已建 | **就地 upsert** + 批次末一次 `refresh()`，推进 `builtVersion`，不重建 |
| `/graph/delEntity`，索引已建 | **就地 delete**，推进 `builtVersion`，不重建 |
| 上述写入，索引尚未构建 | no-op，首次查询构建时一并收录 |
| `/graph/addEntitySchema` | `invalidate()` → 下次冷查询重建 |
| 绕过服务层直接改图 | 无钩子，但 `vertexVersion` 已变 → 下次冷查询比对失败 → 重建 |
| 仅写边 | 只 bump `version`、不 bump `vertexVersion` → **不触发重建** |
| 写入实体的索引内容变为空 | 就地 delete 掉原文档（非文档实体不应留在索引里） |
| accessor 返回 `VERSION_UNSUPPORTED` | 每次冷查询重建，退化为每查询重建行为 |

文本化缓存的清空动作发生在下一次 `getEntityIndex()` 调用中，不是写入时立即执行。

### 3.7 检索与校验原子化

`ResidentSearchIndex.searchWithIndex()` 在**同一把锁内**完成「确保索引有效」与「检索」。拆成两次调用会留下窗口：并发写入可以在两者之间使索引失效，让查询落到不存在的索引上。

### 3.8 仅冷路径使用常驻索引

热路径（子图非空）的语义是「**在子图扩展集内取 top-30**」。改为查全局索引再与扩展集求交，得到的是「全图 top-30 ∩ 扩展集」，结果不同。因此热路径保留一次性小索引，这是语义要求。其代价受控：扩展集规模受子图大小 × 度数约束，且同样受益于文本化缓存。

### 3.9 NRT 刷新替代 `close()`

索引长期存活就不能关闭 writer。`SearchStore.refresh()`：

```java
public void refresh() throws IOException {
    if (writeStats && pendingWrite) { writer.commit(); pendingWrite = false; }
    if (!readStats) { reader = DirectoryReader.open(directory); ...; return; }
    DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
    if (newReader != null) { reader.close(); reader = newReader; searcher = new IndexSearcher(reader); }
}
```

`close()` 只负责真正释放。`pendingWrite` 标记避免无写入时的空 commit。空索引场景下 `DirectoryReader.open` 抛出的 `IndexNotFoundException` 按原路径向上传递，由 `GraphSearchStore.search()` 捕获并返回空列表。

### 3.10 参照实现

每查询重建的逻辑保留为 `SessionOperator.searchWithGlobalGraphByRebuild()`（package-private），用于等价性测试的对照组，以及未提供常驻索引时的兜底。`SessionOperator` 的两参数构造函数保持可用。

---

## 4. 改动点

### 4.1 新增

| 文件 | 行数 | 职责 |
|---|---|---|
| `operator/ResidentSearchIndex.java` | 256 | 按图常驻的关键词索引：`ensureGlobalIndex()` 懒构建 + 版本校验、`searchWithIndex()` 校验与检索同锁、`onEntitiesUpserted()` / `onEntitiesRemoved()` 就地增量维护、`invalidate()` 整体失效；暴露 `buildCount` / `upsertCount` / `removeCount` / `indexedEntityNum` 供测试与观测 |
| `test/operator/ResidentSearchIndexTest.java` | 455 | 读写等价性与性能、插入 / 更新 / 删除就地生效、幂等、未通知改图触发重建、边写入不失效、写查交替、缓存计数，10 例 |
| `test/operator/EmbeddingCandidateSetTest.java` | 153 | 向量候选集两条收集路径的等价性，1 例 |

### 4.2 修改（17 个文件，+565/−57）

**索引与检索**

| 文件 | 改动 |
|---|---|
| `operator/SearchStore.java` | 新增 `refresh()`（`commit()` + `openIfChanged()`）与 `ensureSearcher()`；新增 `updateDoc()` / `deleteDoc()`（by term）；`addDoc(kv, exactField)` 支持把主键写成不分词 `StringField`；`reader` 类型 `IndexReader` → `DirectoryReader`；新增 `pendingWrite` 标记；`close()` 只做真正释放 |
| `operator/GraphSearchStore.java` | 文档带 `SearchConstants.KEY` 主键；新增 `upsertVertex()` / `upsertEdge()` / `removeEntity()` / `refresh()`（吞掉空索引的 `IndexNotFoundException`）；抽出 `vertexDoc()` / `edgeDoc()` / `writeDoc()` 去重；`store` 改 `final` |
| `operator/SearchConstants.java` | 新增 `KEY` 字段名 |
| `operator/SessionOperator.java` | 新增三参构造接收 `ResidentSearchIndex`；冷路径改走 `searchWithIndex()`；全局检索调用移入冷分支；原逻辑重命名为 `searchWithGlobalGraphByRebuild()`；热路径 `close()` → `refresh()`，检索后 `closeQuietly()` |
| `operator/EmbeddingOperator.java` | 全局检索调用移入冷分支；抽出 `collectGlobalCandidates()`，优先用 `indexStore.getIndexedEntities()` 并按图解析每个实体（过滤已删除的残留索引项、取当前顶点对象），不可用时退回全图扫描 |
| `index/IndexStore.java` | 新增可选 `default Collection<GraphEntity> getIndexedEntities()`，返回 `null` 表示无法枚举 |
| `index/EmbeddingIndexStore.java` | 实现 `getIndexedEntities()`，返回 `indexStoreMap.keySet()` 只读视图 |
| `index/EntityAttributeIndexStore.java` | 版本感知的有界 LRU 记忆化（`LinkedHashMap` accessOrder + `removeEldestEntry`）；`invalidateCache()` / `invalidateCache(entity)`；`cacheHit` / `cacheMiss` / `cacheSize` 观测；`initStore()` 顺带清缓存；版本不可用时不缓存 |

**图版本号**

| 文件 | 改动 |
|---|---|
| `graph/io/MemoryGraph.java` | 新增 `version` / `vertexVersion`（`AtomicLong`）及 `getVersion()` / `getVertexVersion()` / `bumpVersion()` / `bumpEdgeVersion()`；点操作走 `bumped()`，边操作走 `edgeBumped()`；`setGraphSchema()` 亦 bump |
| `graph/GraphAccessor.java` | 新增常量 `VERSION_UNSUPPORTED` 与 `default getGraphVersion()` / `getVertexVersion()` |
| `graph/LocalMemoryGraphAccessor.java` | 覆写两个版本方法，委托 `MemoryGraph` |
| `graph/MemoryMutableGraph.java` | `addVertexSchema()` / `addEdgeSchema()` 直接改 `graph.entities`，补 `bumpVersion()` |
| `verbalization/VerbalizationFunction.java` | 新增 `default getSourceVersion()` |
| `verbalization/SubgraphSemanticPromptFunction.java` | 覆写 `getSourceVersion()`，透传 accessor 版本 |

**服务层**

| 文件 | 改动 |
|---|---|
| `GraphMemoryServer.java` | 新增 `IdentityHashMap<IndexStore, ResidentSearchIndex>`；`addIndexStore()` 为关键词索引存注册常驻索引；`search()` 注入常驻索引；新增 `onEntitiesUpserted()` / `onEntitiesRemoved()` / `onSchemaChanged()` |
| `GeaFlowMemoryServer.java` | `/graph/insertEntity` 走 upsert；`/graph/delEntity` 走 delete；只有 `/graph/addEntitySchema` 走整体失效 |
| `common/config/Constants.java` | 新增 `ENTITY_ATTRIBUTE_INDEX_CACHE_MAX_SIZE = 200000` |

---

## 5. 测试结果

环境：macOS arm64、OpenJDK 21.0.11、Maven 3.9.16、Lucene 8.11.2、`topN = 30`。性能数据为连续 3 轮取值范围。

### 5.1 正确性

`ResidentSearchIndexTest`（10 例）+ `EmbeddingCandidateSetTest`（1 例），全部通过：

| 断言 | 说明 |
|---|---|
| 召回等价（常驻 vs 重建） | 10000 顶点、10 组查询，结果集完全一致 |
| 召回等价（有缓存 vs 无缓存） | 文本化缓存不改变召回 |
| `buildCount == 1` | 10 次查询后全图索引只构建 1 次 |
| 插入就地生效 | 新点写入后立即可检索，`upsertCount == 1`，文档数 +1，**不重建** |
| 更新就地生效 | 新内容可检索，**被替换的旧文档不再可检索**，文档数不变，**不重建** |
| 删除就地生效 | 被删文档不再可检索，`removeCount == 1`，文档数 −1，**不重建** |
| upsert 幂等 | 同一实体重复上报 3 次，文档数不变、无重复文档、不重建 |
| 未通知改图触发重建 | 绕过索引直接改图后，版本兜底强制重建并返回新内容（`buildCount == 2`） |
| 边写入不失效 | 写边后 `buildCount` 不变 |
| 写查交替全程不重建 | 40 轮「写入 + 查询」，每轮召回与失效重建方案逐轮一致，全程 `buildCount == 1` |
| 缓存计数 | 首次 miss、再次 hit、单条失效后再次 miss |
| 向量候选集等价 | 枚举索引实体 vs 全图扫描，5 组查询结果逐位一致；数据集含「未索引」与「已索引但无向量」两类顶点 |

结果比对用集合而非列表：每查询重建方案从 `HashMap` 迭代喂 Lucene，文档顺序不确定，并列打分的顺序不稳定。测试查询的命中数控制在 `topN = 30` 以内，规避截断带来的顺序敏感。

### 5.2 读性能：10000 顶点 / 10 次查询

| 配置 | 每查询耗时 |
|---|---|
| A 每查询重建 + 无文本化缓存 | 98.4 ~ 119.5 ms |
| B 每查询重建 + 文本化缓存 | 88.7 ~ 91.8 ms |
| C 常驻索引（稳态） | **0.41 ~ 0.46 ms** |

- C 的一次性构建 84.7 ~ 104.1 ms，仅首次查询承担，稳态相比 A 约 **200 ~ 280 倍**
- B 的缓存命中 90000/100000，仅带来约 20% 改善 —— 本用例内容规模下主要成本是 Lucene 建索引而非文本化，索引常驻是主因，缓存是次要项

**基准场景前提**（该数字的适用边界）：合成图、单一顶点标签、无边、短文本单属性、未设 `PromptFormatter`；测量期间图只读；仅测冷路径全局检索本身，不含 `apply()` 其余部分、会话处理、结果 verbalize 与 HTTP 开销。

### 5.3 写性能：5000 顶点、40 轮「写入 + 查询」交替

就地增量维护存在的理由所在。对照组为失效重建方案（每次写入后 `invalidate()`，下次查询重建）：

| 配置 | 每轮耗时 | 全量构建次数 |
|---|---|---|
| D 写入即失效，查询时重建 | 44.5 ~ 47.1 ms | 41 |
| E 就地增量维护 | **1.60 ~ 1.81 ms** | **1** |

约 **26 倍**，且构建次数与写入次数解耦。每轮召回逐轮比对一致。

E 的每轮 1.7 ms 高于纯读稳态的 0.4 ms，原因是每轮 `refresh()` 产生一个新的 Lucene 段，段数量增长会拖慢检索 —— 与 Elasticsearch 需要靠 `refresh_interval` 攒批并依赖后台合并控制段数是同一个原因。当前按批次刷新（一次 HTTP 请求一次），见 §6.5。

### 5.4 规模敏感性

| 顶点数 | 每查询重建 | 常驻索引稳态 |
|---|---|---|
| 5000 | 43.0 ~ 47.8 ms | 0.28 ~ 0.43 ms |
| 20000 | 170.2 ~ 175.5 ms | 0.41 ~ 0.54 ms |

顶点数 4 倍 → 重建路径约 3.7 倍（线性，确认 O(V)）；常驻路径基本持平。延迟特征从「随图规模线性增长」变为「基本不随图规模增长」。

### 5.5 既有回归用例

| 测试 | 每查询重建 | 常驻索引 |
|---|---|---|
| `MutableGraphTest`（多轮会话 + 频繁改图） | 0.887 s | **0.186 s** |
| `GraphMemoryTest`（LDBC 数据集，严格内容断言） | 0.599 s | 0.536 s |
| `MemoryServerTest`（HTTP 端到端，532 chunk 导入） | 5.717 s | 5.982 s |

`GraphMemoryTest` 的严格内容断言全部通过，是召回未变化的额外佐证。`MemoryServerTest` 未获益，原因见 §6.1。

### 5.6 全量验证

`mvn -B -pl geaflow-ai -am clean install`：Reactor 12 个模块全部 SUCCESS；`geaflow-ai` **17 个测试通过，0 失败 0 错误**；Checkstyle **0 违规**；Apache RAT **Unapproved 0**。

---

## 6. 已知限制

### 6.1 consolidate 写入路径每次插入重建全部检索状态

`KeywordRelationFunction.eval()` 每次调用都 `new EntityAttributeIndexStore()` + `new GraphMemoryServer()`，然后对无关联边的顶点做全局检索。它由 `ConsolidateServer` 在每次 `/graph/insertEntity` 时执行，因此**单次插入代价 O(V)、整体导入 O(V²)**。

实测：`MemoryServerTest` 导入 532 个 chunk，触发 532 次全图索引构建（单次从 2 ms 增至 9 ms）。这是 §5.5 中该测试无改善的原因，也是目前**唯一一条仍在按 O(V) 重建索引的路径**。

修复需让 consolidate 复用按图的检索状态并以增量方式接收新实体，会改动 `ConsolidateFunction.eval` 的契约，建议单独立项。

### 6.2 向量检索无 ANN

`EmbeddingIndexStore` 仍是 `HashMap` + 全候选集余弦计算。本特性只消除了「为组装候选集而扫全图」的开销，算法复杂度仍是 O(N·d)。引入 ANN 需先解决 Lucene 8.11.2 → 9.8.0 与随之而来的 JDK 11 约束（`geaflow-store-vector` 的 `GraphVectorIndex` 正因此被 `-Pjdk8` CI 构建排除），建议先做可插拔 SPI。

### 6.3 常驻索引无内存上界

文本化缓存有 LRU 上界（`Constants.ENTITY_ATTRIBUTE_INDEX_CACHE_MAX_SIZE`，默认 20 万条），Lucene `ByteBuffersDirectory` 内存索引没有。常驻索引与缓存都是新增的稳态堆占用（原来是瞬态的），大图上需评估改用磁盘 `Directory`，默认缓存条数也可能偏大。

### 6.4 并发粒度粗

`ResidentSearchIndex` 用单个 `synchronized (lock)` 覆盖构建、写入与检索，检索会被构建阻塞。`GraphMemoryServer` 的 `residentIndexes` 也未做并发保护 —— 与服务端本身「全局静态状态、无并发保护」的现状一致，因此不是当前瓶颈，但服务化时需换成读写锁并细化粒度。

### 6.5 无段合并与刷新攒批

Lucene 删除只打标记，空间靠段合并回收；每次 `refresh()` 又新增一个段。目前既没有主动 `forceMergeDeletes()`，也没有跨批次的刷新攒批策略，长期高频写入下段数与被标记删除的文档会累积，检索随之变慢（§5.3 中 E 的 1.7 ms 已体现）。成熟系统靠后台合并线程 + 刷新间隔解决，此处需要一个按段数或删除比例触发的合并策略。

### 6.6 版本号仅内存图实现

只有 `MemoryGraph` / `LocalMemoryGraphAccessor` 上报版本。未来 `GeaFlowStateGraphAccessor` 若不实现 `getVertexVersion()`，常驻索引会退化为每查询重建 —— 安全但无收益。接引擎时须一并实现版本上报。

---

## 7. 后续建议

按投入产出排序：

1. **打通 HTTP 层 embedding 通路** —— `GeaFlowMemoryServer.createGraph()` 未注册 `EmbeddingIndexStore`，`execQuery()` 也从不产生 `EmbeddingVector`，导致线上路径的向量检索完全没生效。改动极小，属功能缺陷而非优化。
2. **修 §6.1 的 consolidate 写入路径** —— 让它复用检索状态，把导入从 O(V²) 降到 O(V)。
3. **加段合并与刷新攒批策略**（§6.5）—— 高频写入场景的长期稳定性。
4. **加前置精确匹配** —— query 命中实体 ID / label 时直接定位，跳过全量比对（对应 hugegraph-ai 的 `_exact_match_vids`）。
5. **缩小索引对象** —— 区分「实体 ID / 名称索引」与「完整文本索引」两级，先在小索引上召回候选再精排（对应 hugegraph-ai 只索引 `graph_vids` 的做法）。
6. **建评测基线** —— 缺少评测集，后续检索质量优化无法验证。

---

## 附：参考来源

- [Lucene's Handling of Deleted Documents — Elastic](https://www.elastic.co/blog/lucenes-handling-of-deleted-documents)
- [Lucene's near-real-time search is fast! — DZone](https://dzone.com/articles/lucenes-near-real-time-search)
- [Elasticsearch refresh_interval 说明 — pulse.support](https://pulse.support/kb/what-is-elasticsearch-refresh-interval)
- [Elasticsearch merge storms — Netdata](https://www.netdata.cloud/guides/elasticsearch/elasticsearch-merge-storms/)
- [Milvus data processing 文档](https://milvus.io/docs/data_processing.md)
- [HNSW 删除与墓碑机制分析 — tianpan.co](https://tianpan.co/blog/2026-05-09-retrieval-cascade-failure-document-deletion-rag)

上述来源内容均经改写与摘要，以符合许可要求。
