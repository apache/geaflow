# Feature：关键词检索索引常驻化与就地增量维护

> 模块：`geaflow-ai`　状态：已实现、已验证

---

## 1. 摘要

| 项 | 内容 |
|---|---|
| 能力 | 关键词检索使用按图常驻的 Lucene 索引；查询路径不做索引构建，写入以增量方式就地维护 |
| 手段 | 索引常驻化 + Lucene 主键 update / delete by term + 文本化结果条目级记忆化 + 版本窗口校验 |
| 读效果 | 10000 顶点，每查询 **0.38 ~ 0.60 ms**（每查询重建方案 101.6 ~ 105.7 ms） |
| 写效果 | 5000 顶点、写查交替，每轮 **1.36 ~ 1.40 ms**（失效重建方案 46.3 ~ 46.6 ms）；全量构建次数 41 → **1** |
| 语义 | 召回结果与每查询重建方案一致，由等价性测试保证 |
| 规模 | 新增 `ResidentSearchIndex`、`VertexVersionWindow`；改动 `geaflow-ai` 内 14 个主干文件 |
| 验证 | `mvn -pl geaflow-ai -am clean install` 全绿，19 个测试通过，Checkstyle 0 违规，RAT Unapproved 0 |

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
- 全量构建期间用一个**局部** `Set<GraphEntity>` 去重，等价于原实现以 `Map` 键去重；该集合随构建结束即丢弃，不作为常驻状态保留（见 §3.11）
- Lucene 查询串、`topN`（`Constants.GRAPH_SEARCH_STORE_DEFAULT_TOPN = 30`）、`StandardAnalyzer` 均不变

文档集、查询、打分器三者相同，因此召回相同。

### 3.4 写入以 Lucene 主键增量维护

Lucene 原生支持增量维护，前提是每个文档有一个可精确定位的 term。因此每个文档带一个不分词主键字段 `SearchConstants.KEY`，取值为 `ModelUtils.getGraphEntityKey(entity)`（顶点 `V{id}{label}`，边 `E{src}{label}{dst}`）—— 这个 key 生成逻辑本已存在，`EmbeddingIndexStore` 就用它做 jsonl 行键。

| 操作 | 实现 | 代价 |
|---|---|---|
| 全量构建 | `addDocument`（扫描保证每顶点只出现一次，无需去重开销） | O(V)，仅首次 |
| 新增 / 更新 | `updateDocument(new Term(KEY, key), doc)` | O(变更量) |
| 删除 | `deleteDocuments(new Term(KEY, key))`，只在段级位图打标记 | O(1) |
| 可见性 | 批次结束后一次 `refresh()`（NRT reader 重开，不 commit，见 §3.9） | O(变更量) |

`updateDocument` 对「已存在」与「不存在」处理一致，`deleteDocuments` 对不存在的 key 也是 no-op，因此 **`onEntitiesUpserted` / `onEntitiesRemoved` 都是幂等的，调用方不需要提供精确增量**，重复上报同一实体不会产生重复文档。这消除了「维护正确性依赖调用方给出准确 delta」的隐式契约（但仍要求调用方给出**完整**的变更集合，见 §3.5）。

服务层因此拆成三个语义明确的入口，而不是一个布尔开关：

```
GraphMemoryServer
  ├─ onEntitiesUpserted(entities, window)   新增与更新，就地 upsert
  ├─ onEntitiesRemoved(entities, window)    删除，就地 delete
  └─ onSchemaChanged()                      无法按实体表达的变更，整体失效
```

只有 schema 变更走整体失效 —— 它会改变每一个实体的 verbalize 结果。

### 3.5 版本窗口：由写方声明变更范围，而非读方事后猜

图可以被绕过服务层直接改写（例如经 `MemoryMutableGraph`），这类变更没有任何钩子会触发。因此由图自身维护版本号：

```
MemoryGraph
  ├─ version        任何变更 +1（点、边、schema）
  └─ vertexVersion  仅点与顶点 schema 变更 +1

GraphAccessor
  ├─ getGraphVersion()    默认 VERSION_UNSUPPORTED (-1)
  └─ getVertexVersion()   默认委托 getGraphVersion()

VerbalizationFunction
  ├─ getSourceVersion()        默认 VERSION_UNSUPPORTED
  └─ getSourceVertexVersion()  默认委托 getSourceVersion()
     SubgraphSemanticPromptFunction 两者分别透传 accessor 的 graph / vertex 版本
```

**关键点：不能事后读当前版本来当作「我已全部应用」。** 就地维护完把 `builtVersion` 推进到 `getVertexVersion()` 的当前值，会把这期间任何未上报的改图一并当作已应用，于是那些文档**永久**缺失 —— 兜底只在「越界改图之后再也没有钩子写入」时才成立，一次正常写入就把它吞掉了。

所以改成由写方声明范围，读方校验：

```java
// 写方
VertexVersionWindow window = server.openVertexVersionWindow();   // 记录写前 vertexVersion
... 改图 ...
server.onEntitiesUpserted(entities, window.seal());              // 记录写后 vertexVersion

// 读方（ResidentSearchIndex.applyWrite）
if (window == null || !window.covers(builtVersion)) {
    invalidateLocked();   // 无法证明这批变更是完整的 → 重建，不冒脏数据的风险
    return;
}
... 就地 upsert / delete ...
builtVersion = window.getTo();
```

`covers()` 要求三件事同时成立：窗口已封（`seal()` 调过）、`from == builtVersion`（这批变更正好接在索引已接受的版本之后）、`to == 当前 vertexVersion`（封窗之后没有别人再动过图）。任一不成立就整体重建。

覆盖情况：

| 场景 | 结果 |
|---|---|
| 只走钩子写入 | `covers()` 成立，全程就地维护，`buildCount` 不增长 |
| 越界改图，之后无钩子写入 | 冷查询时 `vertexVersion != builtVersion` → 重建 |
| 越界改图，之后有钩子写入 | `from != builtVersion` → 重建（**修复前会被吞掉**，有回归测试 `testUnreportedMutationIsNotSwallowedByALaterReportedWrite`） |
| 封窗后到钩子执行之间有人改图 | `to != 当前版本` → 重建 |
| 写方自己的改图与封窗之间有人改图 | **未覆盖**，需要写方之间自行串行化，见 §6.4 |

**版本不可用时自动降级**：返回 `VERSION_UNSUPPORTED` 的 accessor（如 `EmptyGraphAccessor`，及任何未实现版本上报的实现）使常驻索引每次重建、缓存完全不启用 —— 退化为每查询重建行为。

`MemoryGraph` 中失败的变更同样 bump 版本：过度失效只是慢，漏失效是正确性缺陷。

**`vertexVersion` 与 `version` 分离**：顶点的 verbalize 只读该顶点自身与 schema（`schema.getPrompt(vertex)`），所以常驻索引与顶点文本化缓存都 watch `vertexVersion`，边写入不会使它们失效 —— 这在 consolidate 场景下有实际意义，一次插入会带来约 30 次 `addEdge`。边的 verbalize 会读取两端顶点（`schema.getPrompt(edge, start, end)`），依赖面更广，因此边的缓存条目 watch 全局 `version`。

**边 schema 只推进 `version`**：新增一个边 schema 不可能改变任何已存在顶点的 verbalize 结果，所以 `MemoryGraph.registerEdgeSchema()` 走 `bumpEdgeVersion()`。否则 consolidate 首次插入时注册 `consolidate_keyword_edge` 会白白使顶点索引失效一次。顶点 schema 与整体 `setGraphSchema()` 仍推进 `vertexVersion`。

schema 变更的写入口收回图内部（`registerVertexSchema` / `registerEdgeSchema`），`bumpVersion` / `bumpEdgeVersion` 因此是 `private` —— 版本推进不再是任何调用方都能触发的公开动作，否则「派生结构是否新鲜」可以被外部随手改写。

### 3.5.1 文本化缓存：条目级版本戳，且不加锁

`EntityAttributeIndexStore` 的记忆化缓存**每个条目带自己的源版本戳**，命中时逐条比对，不一致就只换这一条。

用「整个缓存共享一个版本、不一致即 `clear()`」会让任何一次写入丢掉此前记忆的全部条目（默认上限 20 万条）—— 恰好是写入最频繁的 consolidate 路径受损最重。回归测试 `testEdgeWriteKeepsMemoizedVertexVerbalizations` 固定这一行为：19 次边写入后顶点条目仍然命中，而改写该顶点本身只淘汰它一条。

**容器是 `ConcurrentHashMap`，读写路径都不持锁。** 被记忆的是纯函数、缓存值不可变，所以互斥对正确性不是必需的：

```java
CachedIndex cached = cache.get(entity);
if (cached != null && cached.version == version) { hit; return cached.vectors; }
List<IVector> computed = computeEntityIndex(entity);   // 在 map 之外算
cache.put(entity, new CachedIndex(computed, version)); // 覆盖旧版本条目，无需先 remove
```

两个线程同时未命中同一实体时会各算一遍，但同一版本下算出的东西相同、谁写进去都成立，输的一方只是白做一次；这比让每次查找都进临界区更合适。同理不需要「先查一次、算完再查一次」的 double-check —— 那个二次检查原本是为了配合「整个缓存共享一个版本」的字段，条目级版本戳之后它已经没有对应的竞态要防。

需要说明的取舍：

- 上限变成**近似**、淘汰**不再是 LRU**（按 map 迭代顺序丢一批）。对纯函数的记忆化，淘汰错一条的代价只是重算一次
- `LinkedHashMap(accessOrder=true)` 的 LRU 语义要求连读操作都独占（`get` 会改动链表），这正是原来那把锁的来源
- 计数器改 `LongAdder`

我**没有**测到可靠的吞吐差异：临界区只有一次 map `get`，在 1/4/8/16 线程下监视器都不是瓶颈，两种实现的差距被运行间抖动盖过（8 线程 3.2M 次查找的 wall time 在两侧都落在 230~340 ms）。所以这条改动的理由是「不必要的互斥就不该有，且读路径上不再留共享阻塞点」，不是性能数字。新增 `testConcurrentVerbalizationLookupsAreConsistent` 固定并发下的一致性：8 线程 × 50 轮 × 200 实体，内容全部一致、计数不重不漏、条目数恰好等于实体数。

因此服务层钩子里**不需要**再逐实体 `invalidateCache(entity)`：写入本身已经让受影响的条目过期，那个循环是纯开销。`invalidateCache()` 只保留给版本无法描述的变更（替换 verbalization function、schema 变更）。

### 3.6 更新与失效时机

全部懒执行，没有定时任务，也没有后台重建线程。

| 时机 | 行为 |
|---|---|
| 首次冷查询 | `ensureGlobalIndex()` 全量构建一次 |
| 后续冷查询，`vertexVersion` 未变 | 直接复用，零构建开销 |
| 后续冷查询，`vertexVersion` 已变 | 整体重建 |
| `/graph/insertEntity`（新增或更新），索引已建，窗口可信 | **就地 upsert** + 批次末一次 `refresh()`，`builtVersion` 推进到 `window.getTo()`，不重建 |
| `/graph/delEntity`，索引已建，窗口可信 | **就地 delete**，同上，不重建 |
| 窗口不可信（缺失 / 未封 / 与 `builtVersion` 不接续 / 封窗后又被改） | `invalidate()` → 下次冷查询重建 |
| 上述写入，索引尚未构建 | no-op，首次查询构建时一并收录 |
| `/graph/addEntitySchema` | `onSchemaChanged()` → 缓存清空 + 索引失效 → 下次冷查询重建 |
| 绕过服务层直接改图 | 无钩子，但 `vertexVersion` 已变 → 下次冷查询比对失败 → 重建 |
| 仅写边（含注册边 schema） | 只 bump `version`、不 bump `vertexVersion` → **不触发重建** |
| 写入实体的索引内容变为空 | 就地 delete 掉原文档（非文档实体不应留在索引里） |
| accessor 返回 `VERSION_UNSUPPORTED` | 每次冷查询重建，退化为每查询重建行为 |

文本化缓存的淘汰发生在下一次 `getEntityIndex()` 命中该条目时，不是写入时立即执行。

### 3.7 检索与校验同一次加锁完成

`ResidentSearchIndex.searchWithIndex()` 在**同一次加锁内**完成「确保索引有效」与「检索」。拆成两次调用会留下窗口：并发写入可以在两者之间使索引失效，让查询落到不存在的索引上。

锁是 `ReentrantReadWriteLock`：快路径（索引已建且版本一致）只持读锁，因此并发查询不互相串行；只有需要构建、失效或写入时才升级到写锁。`SearchStore` 的 reader / searcher 字段为此声明成 `volatile`，并保证「每批写入后必定 `refresh()`」，使读路径上的 `ensureSearcher()` 在稳态下是 no-op、不会去改动 store。

### 3.8 仅冷路径使用常驻索引

热路径（子图非空）的语义是「**在子图扩展集内取 top-30**」。改为查全局索引再与扩展集求交，得到的是「全图 top-30 ∩ 扩展集」，结果不同。因此热路径保留一次性小索引，这是语义要求。其代价受控：扩展集规模受子图大小 × 度数约束，且同样受益于文本化缓存。

### 3.9 NRT 刷新替代 `close()`，且不做 `commit()`

索引长期存活就不能关闭 writer。`SearchStore.refresh()` 直接从 writer 开 reader：

```java
public void refresh() throws IOException {
    if (writeStats) {
        if (readStats && nearRealTimeReader) {
            DirectoryReader newReader = DirectoryReader.openIfChanged(reader, writer, true);
            ... // 换 reader / searcher，关旧 reader
        } else {
            reader = DirectoryReader.open(writer);   // 首次：NRT reader
            ...
        }
        pendingWrite = false;
        return;
    }
    // 无 writer（空索引）时才从 directory 开，IndexNotFoundException 由 GraphSearchStore 吞掉
    ...
}
```

**为什么不 commit**：`ByteBuffersDirectory` 是纯内存目录，commit point 换不到任何持久性，只是每批写入多做一次工作。实测去掉 commit 后写查交替从 2.14 ~ 2.42 ms/轮 降到 1.72 ~ 1.86 ms/轮（同机同轮次对比）。writer 关闭时 Lucene 自身会 commit，所以 `close()` 之后目录仍是可读的。

`close()` 只负责真正释放，并把 writer / reader 引用清空。`initWriter()` 每次新建 `IndexWriterConfig` —— Lucene 禁止把已交给某个 writer 的 config 再交给下一个，复用会抛 `IllegalStateException`；这个类既然对外宣称长生命周期，就不能留这种「close 后不能再用」的隐式陷阱。

### 3.10 参照实现

每查询重建的逻辑保留为 `SessionOperator.searchWithGlobalGraphByRebuild()`（package-private），用于等价性测试的对照组，以及未提供常驻索引时的兜底。`SessionOperator` 的两参数构造函数保持可用。

### 3.11 常驻状态只有索引本身

`ResidentSearchIndex` 不再额外保留 `Set<GraphEntity> indexedEntities`：

- 「实体索引内容变空 → 删掉旧文档」原先靠这个集合判断是否曾经收录过，但 delete-by-term 本身幂等，无条件删一次即可
- 文档数改为直接问 Lucene（`SearchStore.numDocs()`），不再维护一个需要自行镜像 update / delete 语义的计数器（原 `GraphSearchStore.entityNum` 既无读取方，upsert 已存在文档时还会多计、删除时不会减）

于是常驻状态就是 Lucene 索引本身。一份与图等大的 `HashSet<GraphEntity>` 在千万点级图上是数百 MB 的稳态堆，去掉它直接改善 §6.3。

---

## 4. 改动点

### 4.1 新增

| 文件 | 职责 |
|---|---|
| `operator/ResidentSearchIndex.java` | 按图常驻的关键词索引：`ensureGlobalIndex()` 懒构建 + 版本校验、`searchWithIndex()` 校验与检索一次加锁（读锁快路径）、`onEntitiesUpserted()` / `onEntitiesRemoved()` 带窗口校验的就地增量维护、`invalidate()` 整体失效；暴露 `buildCount` / `upsertCount` / `removeCount` / `indexedEntityNum` 供测试与观测 |
| `graph/VertexVersionWindow.java` | 一批变更所声明的顶点版本区间：`open()` 记写前版本、`seal()` 记写后版本、`covers(acceptedVersion)` 判定这批变更是否可信为完整 |
| `test/operator/ResidentSearchIndexTest.java` | 读写等价性与性能、插入 / 更新 / 删除就地生效、幂等、未通知改图触发重建（含被后续写入吞掉的回归）、边写入不失效且不清缓存、写查交替、缓存计数、无锁缓存并发一致性，13 例 |
| `test/operator/EmbeddingCandidateSetTest.java` | 向量候选集两条收集路径的等价性，1 例 |

### 4.2 修改

**索引与检索**

| 文件 | 改动 |
|---|---|
| `operator/SearchStore.java` | 新增 `refresh()`（NRT `DirectoryReader.open(writer)` / `openIfChanged(reader, writer, true)`，不 commit）与 `ensureSearcher()`；新增 `updateDoc()` / `deleteDoc()`（by term）、`numDocs()`；`addDoc(kv, exactField)` 支持把主键写成不分词 `StringField`；`reader` 类型 `IndexReader` → `DirectoryReader`；reader / searcher / 状态标记改 `volatile` 以支持并发检索；`initWriter()` 每次新建 `IndexWriterConfig`；移除未使用的 `getConfig()`；`close()` 只做真正释放并清空引用 |
| `operator/GraphSearchStore.java` | 文档带 `SearchConstants.KEY` 主键；新增 `upsertVertex()` / `upsertEdge()` / `removeEntity()` / `refresh()`（吞掉空索引的 `IndexNotFoundException`）/ `getDocNum()`；抽出 `vertexDoc()` / `edgeDoc()` / `writeDoc()` 去重；缓存 schema 的点 / 边 label 集合，不再每次检索用 stream 重算；删除只写不读且计数错误的 `entityNum` |
| `operator/SearchConstants.java` | 新增 `KEY` 字段名 |
| `operator/SessionOperator.java` | 新增三参构造接收 `ResidentSearchIndex`；冷路径改走 `searchWithIndex()`；全局检索调用移入冷分支；原逻辑重命名为 `searchWithGlobalGraphByRebuild()`；热路径 `close()` → `refresh()`，检索后 `closeQuietly()` |
| `operator/EmbeddingOperator.java` | 全局检索调用移入冷分支；抽出 `collectGlobalCandidates()`，优先用 `indexStore.getIndexedEntities()` 并按图解析每个实体（过滤已删除的残留索引项、取当前顶点对象），不可用时退回全图扫描 |
| `index/IndexStore.java` | 新增可选 `default Collection<GraphEntity> getIndexedEntities()`，返回 `null` 表示无法枚举 |
| `index/EmbeddingIndexStore.java` | 实现 `getIndexedEntities()`，返回 key 集合的**快照**（活视图会在并发写入时抛 `ConcurrentModificationException`） |
| `index/EntityAttributeIndexStore.java` | 记忆化改 `ConcurrentHashMap`，读写路径均不持锁、无 double-check（见 §3.5.1）；**每条目带源版本戳**、逐条比对逐条覆盖；顶点条目 watch `getSourceVertexVersion()`、边条目 watch `getSourceVersion()`；按近似上限批量淘汰（`enforceBound()`）；计数器改 `LongAdder`；`invalidateCache()` / `invalidateCache(entity)`；上限直接读 `Constants`，不再在构造期拷进 `final` 字段；版本不可用时不缓存 |

**图版本号**

| 文件 | 改动 |
|---|---|
| `graph/io/MemoryGraph.java` | 新增 `version` / `vertexVersion`（`AtomicLong`）及 `getVersion()` / `getVertexVersion()`；点操作走 `bumped()`，边操作走 `edgeBumped()`；新增 `registerVertexSchema()`（推进 `vertexVersion`）/ `registerEdgeSchema()`（只推进 `version`）；`bumpVersion()` / `bumpEdgeVersion()` 收为 `private` |
| `graph/GraphAccessor.java` | 新增常量 `VERSION_UNSUPPORTED` 与 `default getGraphVersion()` / `getVertexVersion()` |
| `graph/LocalMemoryGraphAccessor.java` | 覆写两个版本方法，委托 `MemoryGraph` |
| `graph/MemoryMutableGraph.java` | `addVertexSchema()` / `addEdgeSchema()` 校验后改为调用 `MemoryGraph.register*Schema()`，不再直接改 `graph.entities` |
| `verbalization/VerbalizationFunction.java` | 新增 `default getSourceVersion()` 与 `default getSourceVertexVersion()` |
| `verbalization/SubgraphSemanticPromptFunction.java` | 分别覆写两者，透传 accessor 的 graph / vertex 版本 |

**服务层**

| 文件 | 改动 |
|---|---|
| `GraphMemoryServer.java` | 新增 `IdentityHashMap<IndexStore, ResidentSearchIndex>`（`synchronizedMap` 包装）；`addIndexStore()` 为关键词索引存注册常驻索引；`search()` 注入常驻索引；新增 `openVertexVersionWindow()` 与带窗口的 `onEntitiesUpserted()` / `onEntitiesRemoved()`、`onSchemaChanged()`；去掉钩子里逐实体清缓存的无效循环 |
| `GeaFlowMemoryServer.java` | `/graph/insertEntity` 改图前开窗、改完即封窗并上报 upsert（consolidate 移到上报之后，使窗口不含它的写入）；`/graph/delEntity` 同理走 delete；只有 `/graph/addEntitySchema` 走整体失效 |
| `common/config/Constants.java` | 新增 `ENTITY_ATTRIBUTE_INDEX_CACHE_MAX_SIZE = 200000` |

---

## 5. 测试结果

环境：macOS arm64、OpenJDK 21.0.11、Maven 3.9.16、Lucene 8.11.2、`topN = 30`。性能数据为连续 3 轮取值范围。

### 5.1 正确性

`ResidentSearchIndexTest`（13 例）+ `EmbeddingCandidateSetTest`（1 例），全部通过：

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
| **未通知改图不被后续写入吞掉** | 越界改图后再走一次正常 upsert，越界写入的文档仍能被检索到，`buildCount == 2`、文档数 22 —— 事后读当前版本的做法会让这条永久缺失 |
| 边写入不失效 | 写边后 `buildCount` 不变 |
| **边写入不清空文本化缓存** | 注册边 schema + 19 次写边后顶点条目仍 hit；改写该顶点本身只淘汰它一条（`cacheSize == 1`） |
| 写查交替全程不重建 | 40 轮「写入 + 查询」，每轮召回与失效重建方案逐轮一致，全程 `buildCount == 1` |
| 缓存计数 | 首次 miss、再次 hit、单条失效后再次 miss |
| **无锁缓存并发一致** | 8 线程 × 50 轮 × 200 实体并发查找：内容全部一致、hit + miss 恰等于调用次数、条目数恰等于实体数 |
| 向量候选集等价 | 枚举索引实体 vs 全图扫描，5 组查询结果逐位一致；数据集含「未索引」与「已索引但无向量」两类顶点 |

结果比对用集合而非列表：每查询重建方案从 `HashMap` 迭代喂 Lucene，文档顺序不确定，并列打分的顺序不稳定。测试查询的命中数控制在 `topN = 30` 以内，规避截断带来的顺序敏感。

### 5.2 读性能：10000 顶点 / 10 次查询

| 配置 | 每查询耗时 |
|---|---|
| A 每查询重建 + 无文本化缓存 | 101.6 ~ 105.7 ms |
| B 每查询重建 + 文本化缓存 | 86.4 ~ 89.2 ms |
| C 常驻索引（稳态） | **0.38 ~ 0.60 ms** |

- C 的一次性构建 97.7 ~ 106.4 ms，仅首次查询承担，稳态相比 A 约 **170 ~ 280 倍**
- B 的缓存命中 90000/100000，仅带来约 18% 改善 —— 本用例内容规模下主要成本是 Lucene 建索引而非文本化，索引常驻是主因，缓存是次要项。注意这个比例只对「图只读」的读基准成立；写入频繁时缓存的价值取决于失效粒度，见 §3.5.1

**基准场景前提**（该数字的适用边界）：合成图、单一顶点标签、无边、短文本单属性、未设 `PromptFormatter`；测量期间图只读；仅测冷路径全局检索本身，不含 `apply()` 其余部分、会话处理、结果 verbalize 与 HTTP 开销。

### 5.3 写性能：5000 顶点、40 轮「写入 + 查询」交替

就地增量维护存在的理由所在。对照组为失效重建方案（每次写入后 `invalidate()`，下次查询重建）：

| 配置 | 每轮耗时 | 全量构建次数 |
|---|---|---|
| D 写入即失效，查询时重建 | 46.3 ~ 46.6 ms | 41 |
| E 就地增量维护 | **1.36 ~ 1.40 ms** | **1** |

约 **33 倍**，且构建次数与写入次数解耦。每轮召回逐轮比对一致。

E 的每轮 1.4 ms 高于纯读稳态的 0.4 ms，成本落在写入与 `refresh()` 一侧，不在检索一侧。把 800 轮写查交替按 100 轮分桶、写与查分开计时可以看到这一点（去 commit 之前的数据）：

| 轮次 | 写入 + refresh | 检索 |
|---|---|---|
| 1..100 | 2.045 ms | 0.754 ms |
| 201..300 | 1.386 ms | 0.396 ms |
| 401..500 | 0.928 ms | 0.215 ms |
| 701..800 | 0.998 ms | 0.260 ms |

检索耗时全程不升反降 —— 段数量增长并没有拖慢检索，Lucene 默认的 `TieredMergePolicy` 已经在后台合并。因此这里的可优化项是 refresh 本身（去掉 commit 已经拿到约 20%，见 §3.9）与跨批次攒批，而不是自己实现段合并策略。参见 §6.5 的更正。

### 5.4 规模敏感性

| 顶点数 | 每查询重建 | 常驻索引稳态 |
|---|---|---|
| 5000 | 45.4 ~ 50.6 ms | 0.31 ~ 0.33 ms |
| 20000 | 176.6 ~ 179.3 ms | 0.51 ~ 0.55 ms |

顶点数 4 倍 → 重建路径约 3.7 倍（线性，确认 O(V)）；常驻路径基本持平。延迟特征从「随图规模线性增长」变为「基本不随图规模增长」。

### 5.5 既有回归用例

| 测试 | 每查询重建 | 常驻索引 |
|---|---|---|
| `MutableGraphTest`（多轮会话 + 频繁改图） | 0.887 s | **0.129 s** |
| `GraphMemoryTest`（LDBC 数据集，严格内容断言） | 0.599 s | 0.592 s |
| `MemoryServerTest`（HTTP 端到端，532 chunk 导入） | 5.717 s | 5.596 s |

`GraphMemoryTest` 的严格内容断言全部通过，是召回未变化的额外佐证。`MemoryServerTest` 基本不获益，原因见 §6.1。

### 5.6 全量验证

`mvn -B -pl geaflow-ai -am clean install`：Reactor 12 个模块全部 SUCCESS；`geaflow-ai` **19 个测试通过，0 失败 0 错误**；Checkstyle **0 违规**；Apache RAT **Unapproved 0**。

---

## 6. 已知限制

### 6.1 consolidate 写入路径每次插入重建全部检索状态

`KeywordRelationFunction.eval()` 每次调用都 `new EntityAttributeIndexStore()` + `new GraphMemoryServer()`，然后对无关联边的顶点做全局检索。它由 `ConsolidateServer` 在每次 `/graph/insertEntity` 时执行，因此**单次插入代价 O(V)、整体导入 O(V²)**。

实测：`MemoryServerTest` 导入 532 个 chunk，触发 532 次全图索引构建（单次从 2 ms 增至 9 ms）。这是 §5.5 中该测试无改善的原因，也是目前**唯一一条仍在按 O(V) 重建索引的路径**。

修复需让 consolidate 复用按图的检索状态并以增量方式接收新实体，会改动 `ConsolidateFunction.eval` 的契约，建议单独立项。

### 6.2 向量检索无 ANN

`EmbeddingIndexStore` 仍是 `HashMap` + 全候选集余弦计算。本特性只消除了「为组装候选集而扫全图」的开销，算法复杂度仍是 O(N·d)。引入 ANN 需先解决 Lucene 8.11.2 → 9.8.0 与随之而来的 JDK 11 约束（`geaflow-store-vector` 的 `GraphVectorIndex` 正因此被 `-Pjdk8` CI 构建排除），建议先做可插拔 SPI。

### 6.3 常驻索引无内存上界，缓存上限是近似值

文本化缓存有条数上限（`Constants.ENTITY_ATTRIBUTE_INDEX_CACHE_MAX_SIZE`，默认 20 万条），Lucene `ByteBuffersDirectory` 内存索引没有。索引本身是新增的稳态堆占用（原来是瞬态的），大图上需评估改用磁盘 `Directory`。

缓存上限有两处不足：按条数而非字节计（20 万条在 LDBC 量级的 verbalization 下可能偏大，需要一个字节预算口径），以及为了让读路径不持锁而放弃了精确上限与 LRU 顺序（§3.5.1）。两者都指向同一个解法：换成带权重与淘汰策略的缓存库（Caffeine 之类），而不是继续在手写 map 上加码。

与图等大的辅助集合已经去掉（§3.11），所以常驻状态只剩索引本身。

### 6.4 并发只做到「查询不互相串行」

`ResidentSearchIndex` 用 `ReentrantReadWriteLock`：并发查询走读锁互不阻塞，但构建与写入仍持写锁、期间检索被阻塞（10000 顶点冷建约 100 ms）。

这里为什么不像文本化缓存那样退到「一个 `volatile` 原子引用、线程之间互不干扰」？因为两者的对象生命周期不同。缓存值是不可变的、被替换后旧值仍然完全可用，读方拿到哪个版本都成立；而 `invalidate()` 会 `close()` 掉 `GraphSearchStore`，一个无锁读方若正好持有它的引用就会撞上 `AlreadyClosedException`。要在无锁的前提下安全回收，必须知道「还有没有人在读」——也就是引用计数，即 Lucene 的 `SearcherManager` / `ReferenceManager`。所以这一步的正确形态是换成 `SearcherManager`，而不是把读写锁直接摘掉。

`GraphMemoryServer.residentIndexes` 已用 `synchronizedMap` 包装，但服务端本身仍是「全局静态 `CACHE`、每请求 new 一个 `MemoryMutableGraph`」，多写方并发时 §3.5 表格最后一行的窗口（写方自己改图与封窗之间被别人插入）无法覆盖 —— 服务化时需要按图的写锁把写入串行化。

### 6.5 无跨批次刷新攒批（原「无段合并」结论已更正）

原先此处认为「段数量增长会拖慢检索，需要自己实现段合并策略」。800 轮写查交替的分桶实测（§5.3）**不支持**这个结论：检索耗时全程不升反降，Lucene 默认的 `TieredMergePolicy` 已经在后台合并段、并按 `deletesPctAllowed` 回收被标记删除的文档，不需要额外的合并策略。

真正剩下的是刷新攒批：目前一批写入（一次 HTTP 请求）刷新一次，`commit()` 已去掉（§3.9），但批量导入场景下仍然是每请求一次 reader 重开。按时间或按变更量延迟刷新（对应 Elasticsearch 的 `refresh_interval`）能把这部分摊薄，代价是可见性延迟。

### 6.6 版本号仅内存图实现

只有 `MemoryGraph` / `LocalMemoryGraphAccessor` 上报版本。未来 `GeaFlowStateGraphAccessor` 若不实现 `getVertexVersion()`，常驻索引会退化为每查询重建 —— 安全但无收益。接引擎时须一并实现版本上报。

---

## 7. 后续建议

按投入产出排序：

1. **打通 HTTP 层 embedding 通路** —— `GeaFlowMemoryServer.createGraph()` 未注册 `EmbeddingIndexStore`，`execQuery()` 也从不产生 `EmbeddingVector`，导致线上路径的向量检索完全没生效。改动极小，属功能缺陷而非优化。
2. **修 §6.1 的 consolidate 写入路径** —— 让它复用检索状态，把导入从 O(V²) 降到 O(V)。
3. **换 `SearcherManager`**（§6.4）—— 让构建期也不阻塞检索，顺带把 reader 生命周期交给引用计数管理。
4. **刷新攒批**（§6.5）—— 批量导入场景摊薄 reader 重开成本。
5. **热路径改用扩展集过滤** —— 用 `TermInSetQuery` 把扩展集的 `KEY` 挂成 filter 查常驻索引，热路径也不必每查询建小索引。注意 BM25 语料统计会从小索引变成全局索引，排序与 §3.8 的重建路径不再严格等价，需要先做 benchmark 与召回对比再决定。
6. **加前置精确匹配** —— query 命中实体 ID / label 时直接定位，跳过全量比对（对应 hugegraph-ai 的 `_exact_match_vids`）。
7. **缩小索引对象** —— 区分「实体 ID / 名称索引」与「完整文本索引」两级，先在小索引上召回候选再精排（对应 hugegraph-ai 只索引 `graph_vids` 的做法）。
8. **建评测基线** —— 缺少评测集，后续检索质量优化无法验证。

---

## 附：参考来源

- [Lucene's Handling of Deleted Documents — Elastic](https://www.elastic.co/blog/lucenes-handling-of-deleted-documents)
- [Lucene's near-real-time search is fast! — DZone](https://dzone.com/articles/lucenes-near-real-time-search)
- [Elasticsearch refresh_interval 说明 — pulse.support](https://pulse.support/kb/what-is-elasticsearch-refresh-interval)
- [Elasticsearch merge storms — Netdata](https://www.netdata.cloud/guides/elasticsearch/elasticsearch-merge-storms/)
- [Milvus data processing 文档](https://milvus.io/docs/data_processing.md)
- [HNSW 删除与墓碑机制分析 — tianpan.co](https://tianpan.co/blog/2026-05-09-retrieval-cascade-failure-document-deletion-rag)

上述来源内容均经改写与摘要，以符合许可要求。
