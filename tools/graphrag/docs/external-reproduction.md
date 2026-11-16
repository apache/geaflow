# GraphRAG 检索：外部复现指南

对应维护者评审的第 1 点：大体量 Benchmark 数据**不随仓库上传**，只提供官方来源链接、
版本与校验和；第三方（或作为操作员的大模型）凭 PR 内容与本文件即可独立复现报告中的全部分数。

## 1. 复现范围与代码版本

| 产物 | 获取方式 | 是否随仓库分发 |
| --- | --- | --- |
| 检索引擎与单元测试 | Maven 构建 | 是 |
| 摄取、发布、基准、消融、扫描工具 | `tools/graphrag/` | 是 |
| 评测数据集（2WikiMultiHopQA、HotpotQA） | 上游下载，第 2 节 | **否**，按需获取 |

| 项 | 值 |
| --- | --- |
| 分支 | `codex/graphrag-adaptive-hybrid-retrieval`（使用最新提交即可） |
| 性能数字的产生提交 | `bba26e94f`（含修复提交 `dbaa0929a`；bundle 的 `build_code_revision` 同此值） |
| 如何确认自己跑的是同一份代码 | 每条运行记录的 `codeRevision` 字段；与上表不一致时数字不可直接比较 |
| 基线代码修订 | `589341fc2`（早期报告所用，仅供对照） |

**注意默认值已变更。** 本指南复现的是**修复后**的数字：融合权重 `0.25 / 1.0`（1:4）、
`high-average-idf=4.0`、`max-hop=1`、`decouple-text-routing=false`。
若要复现 20261112 报告中的旧数字，需先设回旧默认：
`GEAFLOW_RETRIEVAL_GRAPH_FUSION_GRAPH_WEIGHT=2.0`、
`GEAFLOW_RETRIEVAL_GRAPH_FUSION_TEXT_WEIGHT=0.25`，并使用该报告对应的代码修订。

## 2. 数据来源、版本与校验和

### 2WikiMultiHopQA

| 字段 | 值 |
| --- | --- |
| 上游地址 | `https://www.dropbox.com/s/npidmtadreo6df2/data.zip?dl=1` |
| 分发形态 | 数据集作者镜像的 **dev** 划分 |
| 本地缓存 | `tmp/graphrag/data/cache/2wikimultihopqa-dev.zip` |
| 文件大小 | 246,135,840 字节 |
| SHA-256 | `e8e57c0aafc4a26d41131e320ebb5afb6f2aca86b8a6e6611b08f52033cb7d04` |
| 使用划分 | `dev` 前 500 条（`--limit 500`，确定性前缀） |
| 许可 | **Apache-2.0**，上游仓库 `https://github.com/Alab-NII/2wikimultihop` 的 LICENSE；manifest 标记 `confirmed-upstream` |

### HotpotQA（distractor）

| 字段 | 值 |
| --- | --- |
| 上游地址 | `https://hotpotqa.github.io/`，distractor 设置 |
| 本地缓存 | `tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json` |
| 文件大小 | 45,938,749 字节 |
| 文件 SHA-256 | `810332ac58d1f5862e20e25825e8b455e92df5965f7bf719878756f2ccd460f7` |
| 使用划分 | distractor `validation` 前 500 条 |
| 许可 | **CC BY-SA 4.0**，`https://hotpotqa.github.io/` 页面以 `rel=license` 链至 `creativecommons.org/licenses/by-sa/4.0/`；manifest 标记 `confirmed-upstream` |

### 缓存目录与 `GRAPH_RAG_CACHE`（必须设置）

manifest 中的 `cache_path` 写作 `${GRAPH_RAG_CACHE}/<文件名>`，由 `os.path.expandvars` 展开。
**不设置该变量时占位符会原样保留，摄取直接报 `cache is missing`**，因此先导出：

```bash
export GRAPH_RAG_CACHE="$(pwd)/tmp/graphrag/data/cache"
```

下载后的数据集就放在该目录下（HotpotQA 放在其子目录 `hotpotqa-hf/`）。
只用现成 bundle 复现分数时不需要它，**从零构建时必需**。

下载后**先校验再运行**：

```bash
sha256sum tmp/graphrag/data/cache/2wikimultihopqa-dev.zip \
          tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json
```

校验和不一致请停止。语料不同，产出的分数就不同，第 7 节的对比也随之失效。

### 两种向量输入（已分离，不可混用）

本模块有**两种格式不同、用途不同**的向量输入。早期版本共用一个 `GEAFLOW_VECTORS`，
会把查询 NPZ 递给按行读取 JSONL 的发布桥接器，现已拆开：

| 输入 | 环境变量 | 格式 | 用途 |
| --- | --- | --- | --- |
| chunk 向量 | `GEAFLOW_CHUNK_VECTORS` | JSONL，每行一个 `chunk_id` + 向量 | **构建向量索引**（发布 bundle 时） |
| 查询向量 | `GEAFLOW_QUERY_VECTORS` | NPZ，含 `ids` 与 `vectors` 两个数组 | **基准评测**（`--vectors`） |

`GEAFLOW_VECTORS` 仅作为查询向量的**废弃别名**保留，不要再用于 chunk 向量。

参考运行使用的文件：

| 数据集 | chunk 向量（JSONL） | 查询向量（NPZ） |
| --- | --- | --- |
| 2WikiMultiHopQA | 由第 4.2 节命令生成 | `tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz` |
| HotpotQA | `tmp/graphrag/data/runs/hotpotqa-chunk-vectors-gte.jsonl` | `tmp/graphrag/data/runs/hotpotqa-query-vectors-gte.npz` |

缺少查询向量文件时，向量通道会**静默退化**为接近零的召回，这是最常见的假阴性之一。

### 向量生成步骤（含确切模型版本）

向量**可以重新生成**，不依赖任何预上传的文件：

| 项 | 值 |
| --- | --- |
| 模型 | `iic/nlp_gte_sentence-embedding_english-small`（GTE 英文 small） |
| 向量版本标记 | `v1.0.0`（写入 manifest 与 bundle 的 `vector_version`） |
| 模型来源 | `https://modelscope.cn/models/iic/nlp_gte_sentence-embedding_english-small` |
| 维度 | 384（由生成脚本输出校验） |
| 运行方式 | 本地加载（`local_files_only=True`），CPU 或 `--device cuda` |
| 池化/截断 | 见 `tools/graphrag/embedding.py`；`--max-length 512` |

```bash
pip install -r tools/graphrag/requirements-embedding.txt

# 1) chunk 向量：输入是 build_release.py 产出的 chunks.jsonl
PYTHONPATH=. python3 tools/graphrag/embed_chunks.py \
  --chunks <release目录>/chunks.jsonl \
  --model ~/.cache/modelscope/iic/nlp_gte_sentence-embedding_english-small \
  --device cpu --batch-size 32 --max-length 512 \
  --output tmp/graphrag/data/runs/<数据集>-chunk-vectors-gte.jsonl

# 2) 查询向量：输入是查询 JSONL（每条含 id 与 question）
PYTHONPATH=. python3 tools/graphrag/embed_queries.py \
  --queries tmp/graphrag/data/queries/<数据集>.jsonl \
  --model ~/.cache/modelscope/iic/nlp_gte_sentence-embedding_english-small \
  --device cpu --batch-size 32 --max-length 512 \
  --output tmp/graphrag/data/runs/<数据集>-query-vectors-gte.npz
```

模型只需从 ModelScope 下载一次，之后离线可用。嵌入只在这两步发生，**检索服务本身不加载模型**。

## 3. 环境要求

| 组件 | 实测版本 | 说明 |
| --- | --- | --- |
| JDK | 21（17+ 亦可） | CI 同时构建 source/target 8 与 11 |
| Maven | 3.8.8 | 离线构建使用 `-o` |
| Python | 3.11 | 依赖按用途拆成三个文件，见下方安装命令 |
| 操作系统 | Linux x86_64 | |
| 网络 | 数据下载完成后不再需要 | 向量已预计算 |

Python 依赖按用途安装（不是只有一个摄取依赖文件）：

```bash
pip install -r tools/graphrag/requirements.txt             # 基准、评估、测试：numpy/pyarrow/pytest
pip install -r tools/graphrag/requirements-ingest.txt      # 摄取 parquet 语料时需要
pip install -r tools/graphrag/requirements-embedding.txt   # 仅重新生成向量时需要 torch/transformers
```

构建与测试（**不需要跳过检查**；许可证检查已修复，见第 4.4 节）：

```bash
mvn -pl geaflow-ai -o -am -DskipTests -Dmaven.javadoc.skip=true package
mvn -pl geaflow-ai -o test                                 # 470 个测试，含 checkstyle 与 RAT
python3 -m pytest tools/graphrag/tests/ -q                 # 118 个测试
```

## 4. 参考复现包与报告分数

### 4.0 报告数字对应的 bundle（当前代码构建，可自行重建）

**本报告的数字已由“当前代码构建的 bundle”产生，不再绑定某个无法重建的历史 bundle。**
两个 bundle 均在 `READY.json` 中记录了 `build_code_revision`，
重复构建会得到相同的版本与边数，因此可以自行从零重建：

| 数据集 | bundle | 版本 | 边数 |
| --- | --- | --- | --- |
| 2WikiMultiHopQA | `2wikimultihopqa-bundle-current` | `76e9e605acf9be79` | 834,797 |
| HotpotQA | `hotpotqa-bundle-current` | `1341e9e53ee11765` | 516,890 |

**方式 A（推荐）：从零重建**。按第 4.1 节的分阶段流程（摄取 → 嵌入 → 发布 → 基准）执行，
产物就是上表中的 bundle。`build_code_revision` 会记录你构建时的 `git rev-parse HEAD`，
与参考值不同并不影响结果一致性——只要构建代码逻辑未变。

**方式 B：复用已构建的 bundle**。本地已有上述目录时，直接把它传给 `GEAFLOW_BUNDLE`；
也可以用 `tools/graphrag/package_reproduction.sh` 把 bundle、查询向量与参考 CSV
打成带校验和的复现包，便于归档或转移。

**与历史参考 bundle 的关系**：早期报告使用的 bundle 图边数不同（2Wiki 640,275 对 834,797），
因为构建代码在其后发生过变更。实测显示这对最终分数影响很小（自适应 MRR 差异 < 1%），
但**本报告已统一采用当前代码可复现的数字**，以便仅凭仓库内容即可复现。


管线分四阶段：摄取为规范 bundle → 原子发布（`READY.json`）→ 启动检索服务 →
对服务运行六方法基准。`reproduce.sh` 驱动这四个阶段。

### 4.1 每个数据集一条命令

```bash
# 2WikiMultiHopQA
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/2wikimultihopqa-bundle
tools/graphrag/reproduce.sh --dataset 2wikimultihopqa --skip-build --limit 500

# HotpotQA
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/hotpotqa-query-vectors-gte.npz
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/hotpotqa-bundle-gte
tools/graphrag/reproduce.sh --dataset hotpotqa --skip-build --limit 500
```

`--skip-build` 复用已有 bundle。

**没有预构建 bundle 时，可以从数据集完整构建**，不需要任何预上传的索引包。
嵌入是独立步骤（chunk 向量依赖摄取产出的 chunks.jsonl），因此完整流程分四段：

```bash
# 0) 依赖与缓存目录
pip install -r tools/graphrag/requirements.txt -r tools/graphrag/requirements-ingest.txt
pip install -r tools/graphrag/requirements-embedding.txt   # 仅生成向量时需要
export GRAPH_RAG_CACHE="$(pwd)/tmp/graphrag/data/cache"    # 不设置会在摄取时报 cache is missing

# 1) 摄取：从下载并校验过的语料生成 release（含 chunks.jsonl、实体与图数据）
PYTHONPATH=. python3 tools/graphrag/build_release.py \
  --manifest tools/graphrag/manifests/<数据集>-v2.json \
  --output-root tmp/graphrag/data/release
#    输出中的 version 字段决定 release 子目录名

# 2) 生成两种向量（命令见第 2 节）
#    chunk 向量读 <release>/<version>/chunks.jsonl；查询向量读查询 JSONL

# 3) 发布 READY bundle（chunk 向量 JSONL 在这里被构建成向量索引）
PYTHONPATH=. python3 tools/graphrag/publish_bundle.py \
  --manifest tools/graphrag/manifests/<数据集>-v2.json \
  --vectors tmp/graphrag/data/runs/<数据集>-chunk-vectors-gte.jsonl \
  --output tmp/graphrag/data/runs/<数据集>-bundle

# 4) 启动服务并运行六方法基准
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/<数据集>-bundle
export GEAFLOW_QUERY_VECTORS=tmp/graphrag/data/runs/<数据集>-query-vectors-gte.npz
tools/graphrag/reproduce.sh --dataset <数据集> --skip-build --limit 500
```

`reproduce.sh` 在不带 `--skip-build` 时会自行执行第 1、3 段，**但必须先用
`GEAFLOW_CHUNK_VECTORS` 提供 chunk 向量 JSONL**；查询向量 NPZ 不参与索引构建。
2WikiMultiHopQA 全量摄取约一小时，嵌入耗时取决于是否有 GPU。

上述分阶段流程已在本仓库做过端到端验证（第 4.5 节）。

每次运行打印运行目录，汇总文件为 `<运行目录>/<数据集>-six-methods.csv`，
逐请求记录为同名 `.jsonl.gz`。参考机器上每数据集 500 查询 × 6 方法（3000 次请求）
约 40 分钟。

### 4.2 为什么 `--answers-url` 是必需的

早期报告所有方法的 `answerF1` 都是 0，那不是测量结果：`/api/v1/retrievals`
**从不返回答案字段**，只有同时调用独立的答案端点才会计出 F1。
`reproduce.sh` 在自行启动服务时会自动接上该端点，否则该列保持为空，
而不是填入一个编造的数值。

直接运行 `benchmark_http.py` 时请同时传两个参数：

```bash
python3 tools/graphrag/benchmark_http.py \
  --url http://127.0.0.1:18080/api/v1/retrievals \
  --answers-url http://127.0.0.1:18080/api/v1/answers \
  ...
```

### 4.3 组件消融与权重扫描

```bash
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/2wikimultihopqa-bundle
export GEAFLOW_QUERIES=tmp/graphrag/data/queries/2wikimultihopqa-dev.jsonl

# 组件消融：8 条件，每次仅覆盖一个路由设置并重启服务，约 50 分钟
tools/graphrag/ablate_adaptive.sh --dataset 2wikimultihopqa --limit 200 --max-candidates 200

# 融合权重扫描：固定 text-weight=1.0，逐个重启服务，约 35 分钟
tools/graphrag/sweep_fusion.sh --dataset 2wikimultihopqa --limit 200
```

**消融必须显式传 `--max-candidates 200`**：脚本的 `multi-hop` 消融会把候选窗口减半，
默认 100 得到 `candidateLimit=50`，与 500 条主报告（100）不可直接比较。

可选的 `growth-4`、`growth-8` 条件用于检验放宽扩散上限是否有多跳收益（实测无收益）。

### 4.4 classpath 与服务进程

三个脚本都支持 `GEAFLOW_CLASSPATH` 指向一份已有的非空 classpath 文件。

**此前“空 classpath”的真正原因已查明并修复**：Maven 会把相对的 `-Dmdep.outputFile`
解析到**模块目录**，于是正确的 classpath 被写进 `geaflow-ai/tmp/...`，
脚本随后读到的目标文件却是空的。这与离线构建无关。脚本现在把输出目录统一转为绝对路径，
并在 classpath 为空时直接报错，而不是启动服务再报 `NoClassDefFoundError`。

这些误入模块目录的产物还曾导致 **RAT 许可证检查失败**（默认构建因此失败）。
根 `pom.xml` 的 RAT 排除现已加入 `**/tmp/**`：运行产物是生成输出，不是需要许可证头的源码。
修复后 `mvn -pl geaflow-ai -o test` 在不跳过任何检查的情况下通过（470 个测试）。

若服务启动失败而端口被上一次运行占用，先清理残留进程再重跑，
否则基准可能打到旧服务上而得到看似合理但属于另一配置的数字。
扫描脚本已检查 ready 响应是否来自本次启动的 JVM。

### 4.5 从零构建的端到端验证记录

为证明上述流程不需要任何预上传产物，用 2WikiMultiHopQA dev 的前 12 条构造了一个小型真实子集，
完整跑通“摄取 → 嵌入 → 发布 → 服务 → 六方法基准”：

| 步骤 | 结果 |
| --- | --- |
| `build_release.py` | 生成 release（171 个顶点、52 个 chunk） |
| `embed_chunks.py` / `embed_queries.py` | 52 条 chunk 向量（JSONL）、12 条查询向量（NPZ，384 维） |
| `publish_bundle.py` | `state=READY`，`vector_source=iic/nlp_gte_sentence-embedding_english-small`、`vector_version=v1.0.0` |
| 六方法基准 | 全部 12/12 请求成功且有证据；`VECTOR_ONLY` 评估了全部 52 个 chunk 向量 |

该验证只证明**管线连通与索引确实由 chunk 向量建立**，不产出可与第 5 节比较的质量分数。
验证产物在 `tmp/graphrag/_work/mini/`。

### 4.6 从零重建的可复现性验证（本报告数字的依据）

本报告数字**不是来自某个不可重建的历史 bundle**，而是来自当前代码构建的 bundle。
为确认这一点，做了完整验证：

1. **从零构建管线可用**：用 2Wiki dev 前 12 条真实子集跑通“摄取 → 嵌入 → 发布 → 服务 → 六方法基准”，
   向量通道评估了全部 52 个 chunk 向量。
2. **全量重建的确定性**：对全部 48,851 个 chunk 重新嵌入并发布后，
   再次用相同代码与输入构建，得到**相同的 bundle 版本与边数**（`76e9e605acf9be79` / 834,797）。
   HotpotQA 为 `1341e9e53ee11765` / 516,890。
3. **与历史参考 bundle 的差异已量化**：

| 对比项 | 结果 |
| --- | --- |
| 文档数 / chunk 数 / 随机种子 | 完全一致（12,576 / 48,851 / 0） |
| `chunk_id`、文本、偏移量 | 前 3001 条零差异（仅 `source_hash` 元数据不同） |
| BM25_ONLY / VECTOR_ONLY / VECTOR_OVERFETCH | 召回、MRR、NDCG 逐位相同 |
| 图边数 | 历史 640,275 → 当前 834,797（+30.4%），源于构建代码在历史 bundle 之后发生过变更 |
| 对最终分数的影响 | **自适应 MRR 差异 < 1%**：2Wiki 0.7867 → 0.7850，HotpotQA 0.8067 → 0.8096 |

**结论**：图快照会随构建代码版本漂移，但对混合排序结果的影响很小；
本报告统一采用**当前代码可复现**的数字，因此仅凭仓库内容即可复现，
不再依赖某个无法重建的历史 bundle。

`build_code_revision` 字段（`READY.json`）记录每次构建的 `git rev-parse HEAD`，
若未来构建逻辑再次变化，可用它检出对应版本重建：

```bash
REV=$(python3 -c "import json;print(json.load(open('<bundle>/READY.json'))['build_code_revision'])")
git checkout "$REV"
# 然后按第 4.1 节的分阶段流程重新摄取、嵌入、发布
```

## 5. 预期结果

以下数值与 `performance-metrics.md` 第 3 节来自**同一次运行**，代码为第 1 节的提交。

### 2WikiMultiHopQA，500 条查询

| 方法 | Recall@10 | MRR | NDCG@10 | F1 | EM | 平均保留 token | 边扫描 | P95 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.7978 | 0.7368 | 0.6979 | 0.00226 | 0.0000 | 2805.7 | 0.00 | 3288 |
| VECTOR_ONLY | 0.7142 | 0.7154 | 0.6406 | 0.00239 | 0.0000 | 2602.8 | 0.00 | 1299 |
| VECTOR_OVERFETCH | 0.7142 | 0.7154 | 0.6406 | 0.00239 | 0.0000 | 2602.8 | 0.00 | 1148 |
| GRAPH_ONLY | 0.4978 | 0.5136 | 0.4435 | 0.00906 | 0.0000 | 843.6 | 46.05 | 1474 |
| FIXED_HYBRID | 0.8415 | 0.7671 | 0.7367 | 0.00241 | 0.0000 | 2676.3 | 36.61 | 3371 |
| ADAPTIVE_HYBRID | 0.8485 | 0.7850 | 0.7478 | 0.00240 | 0.0000 | 2699.4 | 30.13 | 3417 |

### HotpotQA distractor，500 条查询

| 方法 | Recall@10 | MRR | NDCG@10 | F1 | EM | 平均保留 token | 边扫描 | P95 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.9673 | 0.8254 | 0.8235 | 0.00247 | 0.0000 | 2854.3 | 0.00 | 1950 |
| VECTOR_ONLY | 0.8197 | 0.7655 | 0.7119 | 0.00236 | 0.0000 | 2779.1 | 0.00 | 714 |
| VECTOR_OVERFETCH | 0.8197 | 0.7655 | 0.7119 | 0.00236 | 0.0000 | 2779.1 | 0.00 | 562 |
| GRAPH_ONLY | 0.3822 | 0.3270 | 0.3056 | 0.00801 | 0.0000 | 656.3 | 20.52 | 510 |
| FIXED_HYBRID | 0.9470 | 0.8003 | 0.7914 | 0.00247 | 0.0000 | 2827.2 | 38.51 | 1999 |
| ADAPTIVE_HYBRID | 0.9260 | 0.8096 | 0.7895 | 0.00246 | 0.0000 | 2815.6 | 33.47 | 2033 |

**容差**：检索是确定性的，Recall/MRR/NDCG/F1/EM/token 与边扫描应**精确复现**。
延迟随硬件变化，P95 仅作数量级参考。

## 6. 如何确认复现成功

按顺序检查，每一项隔离一种不同的失败模式。

| 检查项 | 期望 | 失败时的含义 |
| --- | --- | --- |
| bundle 已发布 | `READY.json` 存在，`status=READY` | 去掉 `--skip-build` 重跑 |
| gold 已加载 | `unresolvedSupportingFacts` 为较小的数（2Wiki 500 条为 28，HotpotQA 为 0） | `--gold` 未解析成功；检查 `GEAFLOW_GOLD` |
| 查询向量已加载 | `VECTOR_ONLY` 召回远高于 0.001 | 缺少 `--vectors`；向量方法静默退化 |
| 答案端点已接上 | 至少一个方法的 `answerF1` 非零 | 缺少 `--answers-url` |
| 路由已生效 | `policyId=deterministic-rule`，ADAPTIVE 的 `meanReplanCount` > 0 | 服务未加载 `retrieval.*` 配置 |
| 语料命名空间一致 | 返回的证据 id 全部存在于 `--chunks` 文件中 | 直接中止，见第 7 节的强制防护 |
| 默认配置如预期 | trace 的 `policyParameters` 中 `graphFusionGraphWeight=0.25`、`graphFusionTextWeight=1.0`、`decoupleTextRouting=false` | 环境变量覆盖了默认值 |
| 确定性 | 两次运行产出完全相同的 Recall/MRR | 语料或 bundle 在两次运行之间变化 |

最常见的**假阴性**是"所有质量列都为空"的运行：那说明 `--gold` 没有解析成功，
而不是引擎失败。

## 7. 评测管线的强制防护（为什么旧数字不可信）

早期报告 BM25 与 VECTOR 召回接近 0.001、graph-only 全面领先，其根因是
**对一份已不存在语料的 chunk id 打分**：

| | 早期运行（20261104） | 本文档 |
| --- | --- | --- |
| 首个证据 id | `0009eeb41e0f...` | `2db9ae3ba834...` |
| 2Wiki BM25 Recall@10 | 0.0022 | 以第 5 节为准 |
| 2Wiki GRAPH_ONLY Recall@10 | 0.5028 | 0.5030 量级 |

只有 graph-only 的数值稳定，这正是问题长期未被发现的原因。
**本文档的数字取代早期报告。**

这类失败现在由管线强制拦截，不再依赖人工发现：

1. **gold 解析率检查**：发送请求前，若超过半数查询的支持事实无法解析即中止
   （`gold does not resolve against ...`）。
2. **证据命名空间检查**：任一响应含有 `--chunks` 中不存在的证据 id 时立即中止，
   报 `evidence id namespace mismatch`，并给出方法、查询 id、样例 id 与修复建议。

第二道必不可少：`unresolvedSupportingFacts` 无法发现这种错误，因为 gold 句子仍能针对
所提供的 chunks 文件解析成功，于是 gold 覆盖看似正常，而返回的证据 id 全部来自另一份语料。

实测：同源 chunks 下 4000/4000 个返回 id 全部命中，不会误报；故意换用另一数据集的
chunks 文件时，第一个响应即被拦下。若服务确实会返回该文件之外的证据 id，
可显式传 `--allow-unknown-evidence-ids` 跳过第二道防护。

## 8. 用大模型辅助复现

检索链路本身不调用任何大模型：答案层是抽取式的，F1 由检索到的 chunk 片段计算。
大模型**只能作为操作员，不能进入测量环节**。

可用的提示词（每条结论都必须对照 CSV 的某一列核实）：

> 我有一个由 `reproduce.sh` 产生的运行目录。请读取 `<数据集>-six-methods.csv`，
> 告诉我哪些列是空的、哪些列有值。**不要推断缺失值。**

> 请比较 ADAPTIVE_HYBRID 与 FIXED_HYBRID 的 `evidenceRecallAtK`，
> 报告绝对差值与相对百分比，并**同时引用两个原始值**。

> `answerF1` 列对所有方法都接近 0.002，而 `unresolvedSupportingFacts` 为 0。
> 我应该先检查哪个接线环节？由哪个文件决定？

**禁止**大模型做的事情：填补缺失指标、推测某个数字"应该是多少"、
在没有提供 CSV 的情况下总结消融实验、或在服务未就绪时继续跑基准。
本报告与配套报告中的每一个数字，都可追溯到 `tmp/graphrag/data/runs/` 下某个 CSV 的一行。

