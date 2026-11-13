# GraphRAG 检索：外部复现指南

本文档使独立第三方能够从干净检出开始，复现 `graphrag-performance-metrics.md`
与 `graphrag-adaptive-ablation.md` 中的每一个数字。文中说明数据从何而来、如何获取、
需要运行什么，以及应当得到什么结果。

本仓库不包含任何评测语料。数据集体量大，且受各自的许可约束，因此以**上游链接、
版本与校验和**的方式引用，而非随仓库分发。

## 1. 复现范围

| 产物 | 生成方式 | 是否在仓库中 |
| --- | --- | --- |
| 检索引擎与单元测试 | Maven | 是 |
| 摄取、基准与压测工具 | `tools/graphrag/` | 是 |
| 评测数据集（2WikiMultiHopQA、HotpotQA） | 上游下载 | **否**——按需获取，不随仓库分发 |

## 2. 数据来源

### 2WikiMultiHopQA

| 字段 | 值 |
| --- | --- |
| 上游地址 | `https://www.dropbox.com/s/npidmtadreo6df2/data.zip?dl=1` |
| 分发形态 | 数据集作者镜像的 2WikiMultiHopQA **dev** 划分 |
| 本地缓存文件 | `tmp/graphrag/data/cache/2wikimultihopqa-dev.zip` |
| 文件大小 | 246,135,840 字节 |
| SHA-256 | `e8e57c0aafc4a26d41131e320ebb5afb6f2aca86b8a6e6611b08f52033cb7d04` |
| 使用划分 | `dev`，前 500 条查询（确定性前缀，`--limit 500`） |
| 许可 | 数据集作者条款；manifest 中标记为 `pending-review` |

使用前请先校验下载：

```bash
sha256sum tmp/graphrag/data/cache/2wikimultihopqa-dev.zip
# 期望值：e8e57c0aafc4a26d41131e320ebb5afb6f2aca86b8a6e6611b08f52033cb7d04
```

### HotpotQA（distractor）

| 字段 | 值 |
| --- | --- |
| 上游地址 | `https://hotpotqa.github.io/`——distractor 设置 |
| 本地缓存文件 | `tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json` |
| 文件大小 | 45,938,749 字节 |
| SHA-256 | `810332ac58d1f5862e20e25825e8b455e92df5965f7bf719878756f2ccd460f7` |
| 使用划分 | distractor `validation`，前 500 条查询 |
| 许可 | 上游 CC BY-SA 4.0；manifest 中标记为 `pending-review` |

```bash
sha256sum tmp/graphrag/data/cache/hotpotqa-hf/hotpotqa-distractor.json
# 期望值：810332ac58d1f5862e20e25825e8b455e92df5965f7bf719878756f2ccd460f7
```

**校验和不一致时请停止后续步骤。** 语料不同，产出的数字就不同，下文的对比也随之失效。

## 3. 环境要求

| 组件 | 实测版本 | 说明 |
| --- | --- | --- |
| JDK | 21.0.12（17+ 亦可） | CI 同时构建 source/target 8 与 11 |
| Maven | 3.8.8 | |
| Python | 3.11 | `tools/graphrag/` 除摄取附加依赖外仅用标准库 |
| 操作系统 | Linux x86_64 | |
| 网络 | 数据下载完成后不再需要 | 向量已预计算 |

先构建一次模块：

```bash
mvn -pl geaflow-ai -o -am -DskipTests -Drat.skip=true -Djacoco.skip=true package
```

运行测试套件：

```bash
mvn -pl geaflow-ai -o test -Drat.skip=true -Djacoco.skip=true   # 463 个测试
python3 -m pytest tools/graphrag/tests/ -q                       # 115 个测试
```

## 4. 复现报告中的数字

管线分四阶段：将原始语料摄取为规范 bundle、原子发布 bundle（`READY.json`）、
启动检索服务、对其运行六方法基准。`reproduce.sh` 驱动这四个阶段。

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

`--skip-build` 复用已有 bundle。若机器上没有 bundle，去掉该参数，
`reproduce.sh` 会先执行摄取、嵌入与发布；2WikiMultiHopQA 预计约一小时。

每次运行会打印其运行目录。汇总文件为 `<运行目录>/<数据集>-six-methods.csv`。

参考机器上的耗时：每数据集 500 条查询 × 6 方法（共 3000 次请求）约 40 分钟。

### 4.2 为什么 `--answers-url` 是必需的

早期报告显示所有方法的 `answerF1` 都是 0。那不是一次测量结果：
`/api/v1/retrievals` 从不返回答案字段，只有同时调用独立的答案端点才会计出 F1。
`reproduce.sh` 在自行启动服务时会自动接上该端点，否则该列保持为空，
而不是填入一个编造的数值。

若直接运行 `benchmark_http.py`，请同时传入两个参数：

```bash
python3 tools/graphrag/benchmark_http.py \
  --url http://127.0.0.1:18080/api/v1/retrievals \
  --answers-url http://127.0.0.1:18080/api/v1/answers \
  ...
```

### 4.3 组件级消融

```bash
export GEAFLOW_VECTORS=tmp/graphrag/data/runs/2wikimultihopqa-query-vectors-gte.npz
export GEAFLOW_BUNDLE=tmp/graphrag/data/runs/2wikimultihopqa-bundle
export GEAFLOW_QUERIES=tmp/graphrag/data/queries/2wikimultihopqa-dev.jsonl
tools/graphrag/ablate_adaptive.sh --dataset 2wikimultihopqa --limit 200
```

共 8 个条件，每次仅覆盖一个路由设置并重启服务。
输出：`<out>/component-ablation.csv` 及各条件子目录。
200 条查询耗时约 50 分钟。

## 5. 期望结果

### 2WikiMultiHopQA，500 条查询

| 方法 | Recall@10 | MRR | NDCG@10 | F1 | EM | 边扫描 | tokens | P95 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.7978 | 0.7368 | 0.6979 | 0.0023 | 0.0000 | 0.0 | 2806 | 2786 |
| VECTOR_ONLY | 0.7142 | 0.7154 | 0.6406 | 0.0024 | 0.0000 | 0.0 | 2603 | 1132 |
| VECTOR_OVERFETCH | 0.7142 | 0.7154 | 0.6406 | 0.0024 | 0.0000 | 0.0 | 2603 | 945 |
| GRAPH_ONLY | 0.5030 | 0.5260 | 0.4506 | 0.0095 | 0.0000 | 45.9 | 842 | 1248 |
| FIXED_HYBRID | 0.8435 | 0.7703 | 0.7388 | 0.0024 | 0.0000 | 36.3 | 2676 | 2867 |
| ADAPTIVE_HYBRID | 0.8358 | 0.7182 | 0.6983 | 0.0024 | 0.0000 | 30.1 | 2682 | 2905 |

### HotpotQA distractor，500 条查询

| 方法 | Recall@10 | MRR | NDCG@10 | F1 | EM | 边扫描 | tokens | P95 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.9673 | 0.8254 | 0.8235 | 0.0025 | 0.0000 | 0.0 | 2854 | 1806 |
| VECTOR_ONLY | 0.8197 | 0.7658 | 0.7121 | 0.0024 | 0.0000 | 0.0 | 2779 | 667 |
| VECTOR_OVERFETCH | 0.8197 | 0.7658 | 0.7121 | 0.0024 | 0.0000 | 0.0 | 2779 | 548 |
| GRAPH_ONLY | 0.3818 | 0.3272 | 0.3056 | 0.0079 | 0.0000 | 20.4 | 658 | 499 |
| FIXED_HYBRID | 0.9460 | 0.8008 | 0.7914 | 0.0025 | 0.0000 | 38.4 | 2828 | 1887 |
| ADAPTIVE_HYBRID | 0.9525 | 0.6970 | 0.7323 | 0.0025 | 0.0000 | 33.7 | 2837 | 1909 |

**容差**：检索是确定性的，Recall/MRR/NDCG 应精确复现。延迟随硬件变化，
P95 仅作数量级参考。

## 6. 如何确认复现成功

请按顺序检查。每一项隔离一种不同的失败模式。

| 检查项 | 期望 | 失败时的含义 |
| --- | --- | --- |
| bundle 已发布 | `READY.json` 存在，`status` = READY | 去掉 `--skip-build` 重跑 |
| gold 已加载 | `unresolvedSupportingFacts` 为较小的数（2Wiki 为 28，HotpotQA 为 0） | `--gold` 未解析成功；检查 `GEAFLOW_GOLD` |
| 查询向量已加载 | `VECTOR_ONLY` 召回远高于 0.001 | 缺少 `--vectors`；向量方法会静默退化 |
| 答案端点已接上 | 至少一个方法的 `answerF1` 非零 | 缺少 `--answers-url` |
| 路由已生效 | `policyId` = `deterministic-rule`，ADAPTIVE 的 `meanReplanCount` > 0 | 服务启动时未加载 `retrieval.*` 配置 |
| 确定性 | 两次运行产出完全相同的 Recall/MRR | 语料或 bundle 在两次运行之间发生了变化 |

最常见的**假阴性**是“所有质量列都为空”的一次运行。
那说明 `--gold` 没有解析成功，而不是引擎失败。

## 7. 与早期报告的关系

早期版本的结果报告 BM25 与 VECTOR 召回接近 0.001，且 graph-only 在所有指标上领先。
那些运行基于语料的一种**不同切分**，其证据标识符在本文档描述的 bundle 中并不存在：

| | 早期运行（`20261104`） | 本文档（`20261112`） |
| --- | --- | --- |
| 首个证据 id | `0009eeb41e0f...` | `2db9ae3ba834...` |
| BM25 Recall@10 | 0.0022 | 0.7978 |
| GRAPH_ONLY Recall@10 | 0.5028 | 0.5030 |
| GRAPH_ONLY 对比 BM25 | 图通道领先 | 文本通道领先 |

只有 graph-only 的数值在两次运行间保持稳定，这正是该问题长期未被发现的原因：
其余指标之所以变化，是因为文本通道被打分的 chunk id 来自一份已不存在的语料。
**本文档的数字取代早期报告。** 任何引用旧数字的场合，请使用上方校验和重新运行。

## 8. 用大模型辅助复现

检索链路本身不调用任何大模型：答案层是抽取式的，F1 由从检索到的 chunk 中
选出的片段计算得到。大模型仍可用于辅助，但**只能作为操作员，不能进入测量环节**。

以下是可用的提示词，前提是每一条结论都必须对照 CSV 的某一列进行核实：

> 我有一个由 `reproduce.sh` 产生的运行目录。请读取 `<数据集>-six-methods.csv`，
> 告诉我哪些列是空的、哪些列有值。**不要推断缺失值。**

> 请比较 ADAPTIVE_HYBRID 与 FIXED_HYBRID 的 `evidenceRecallAtK`，
> 报告绝对差值与相对百分比，并**同时引用两个原始值**。

> `answerF1` 列对所有方法都是 0.002，而 `unresolvedSupportingFacts` 为 0。
> 我应该先检查哪个接线环节？由哪个文件决定？

**禁止**使用大模型做的事情：填补缺失指标、推测某个数字“应该是多少”、
或在未提供 CSV 的情况下总结消融实验。两份配套报告中的每一个数字，
都可追溯到 `tmp/graphrag/data/runs/` 下某个 CSV 的一行。

## 9. 如何报告复现失败

请包含：运行目录名、两个数据集的校验和、第 6 节中失败的那一项检查，
以及 `service.log` 的前 20 行。缺少校验和时，无法区分是代码差异还是语料差异。