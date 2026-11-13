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
| 分支 | `codex/graphrag-adaptive-hybrid-retrieval` |
| 复现所用提交 | `fee160853`（含修复提交 `dbaa0929a`） |
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
| 许可 | 数据集作者条款；manifest 标记 `pending-review` |

### HotpotQA（distractor）

| 字段 | 值 |
| --- | --- |
| 上游地址 | `https://hotpotqa.github.io/`，distractor 设置 |
| 本地缓存 | `tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json` |
| 文件大小 | 45,938,749 字节 |
| 文件 SHA-256 | `810332ac58d1f5862e20e25825e8b455e92df5965f7bf719878756f2ccd460f7` |
| 使用划分 | distractor `validation` 前 500 条 |
| 许可 | 上游 CC BY-SA 4.0；manifest 标记 `pending-review` |

下载后**先校验再运行**：

```bash
sha256sum tmp/graphrag/data/cache/2wikimultihopqa-dev.zip \
          tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json
```

校验和不一致请停止。语料不同，产出的分数就不同，第 7 节的对比也随之失效。

### 预计算向量

向量不重新训练、不在线计算，使用已发布的 `gte` 向量文件：

| 数据集 | 文件 |
| --- | --- |
| 2WikiMultiHopQA | `tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz` |
| HotpotQA | `tmp/graphrag/data/runs/hotpotqa-query-vectors-gte.npz` |

缺少向量文件时，向量通道会**静默退化**为接近零的召回，这是最常见的假阴性之一。

## 3. 环境要求

| 组件 | 实测版本 | 说明 |
| --- | --- | --- |
| JDK | 21（17+ 亦可） | CI 同时构建 source/target 8 与 11 |
| Maven | 3.8.8 | 离线构建使用 `-o` |
| Python | 3.11 | 基准工具仅用标准库；向量读取需要 `numpy` |
| 操作系统 | Linux x86_64 | |
| 网络 | 数据下载完成后不再需要 | 向量已预计算 |

构建与测试：

```bash
mvn -pl geaflow-ai -o -am -DskipTests -Drat.skip=true -Djacoco.skip=true package
mvn -pl geaflow-ai -o test -Drat.skip=true -Djacoco.skip=true   # 470 个测试
python3 -m pytest tools/graphrag/tests/ -q                       # 118 个测试
```

## 4. 复现报告中的分数

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

`--skip-build` 复用已有 bundle。没有 bundle 时去掉该参数，脚本会先摄取、嵌入并发布；
2WikiMultiHopQA 预计约一小时。

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
离线环境下 `dependency:build-classpath` 可能**静默产出空文件**；脚本现在会直接报错，
而不是带着空 classpath 启动服务再报 `NoClassDefFoundError`。

若服务启动失败而端口被上一次运行占用，先清理残留进程再重跑，
否则基准可能打到旧服务上而得到看似合理但属于另一配置的数字。

## 5. 预期结果

以下数值与 `performance-metrics.md` 第 3 节来自**同一次运行**，代码为第 1 节的提交。

### 2WikiMultiHopQA，500 条查询

| 方法 | Recall@10 | MRR | NDCG@10 | F1 | EM | 平均保留 token | 边扫描 | P95 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.7978 | 0.7368 | 0.6979 | 0.00226 | 0.0000 | 2805.7 | 0.00 | 3332 |
| VECTOR_ONLY | 0.7142 | 0.7154 | 0.6406 | 0.00239 | 0.0000 | 2602.9 | 0.00 | 1282 |
| VECTOR_OVERFETCH | 0.7142 | 0.7154 | 0.6406 | 0.00239 | 0.0000 | 2602.9 | 0.00 | 1166 |
| GRAPH_ONLY | 0.5030 | 0.5260 | 0.4506 | 0.00951 | 0.0000 | 841.9 | 45.85 | 1539 |
| FIXED_HYBRID | 0.8435 | 0.7703 | 0.7388 | 0.00242 | 0.0000 | 2676.5 | 36.26 | 3484 |
| ADAPTIVE_HYBRID | 0.8495 | 0.7867 | 0.7483 | 0.00240 | 0.0000 | 2699.4 | 30.10 | 3497 |

### HotpotQA distractor，500 条查询

| 方法 | Recall@10 | MRR | NDCG@10 | F1 | EM | 平均保留 token | 边扫描 | P95 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.9673 | 0.8254 | 0.8235 | 0.00247 | 0.0000 | 2854.3 | 0.00 | 2073 |
| VECTOR_ONLY | 0.8197 | 0.7658 | 0.7121 | 0.00236 | 0.0000 | 2779.1 | 0.00 | 776 |
| VECTOR_OVERFETCH | 0.8197 | 0.7658 | 0.7121 | 0.00236 | 0.0000 | 2779.1 | 0.00 | 617 |
| GRAPH_ONLY | 0.3818 | 0.3272 | 0.3056 | 0.00793 | 0.0000 | 658.2 | 20.42 | 553 |
| FIXED_HYBRID | 0.9460 | 0.8008 | 0.7914 | 0.00247 | 0.0000 | 2827.8 | 38.42 | 2162 |
| ADAPTIVE_HYBRID | 0.9270 | 0.8067 | 0.7892 | 0.00247 | 0.0000 | 2816.3 | 33.66 | 2184 |


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

