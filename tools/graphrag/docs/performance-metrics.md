# GraphRAG 检索：性能与成本指标报告

对应维护者评审的第 2 点：把**成本与效果放在一起**汇报，补充 token 成本、命中率与 F1，
并给出自适应方案相对每一种单路方案的**具体**提升幅度——包括提升有限甚至为负的情形。

本报告数字来自 PR 代码（提交 `fee160853`，含修复 `dbaa0929a`）的两个 500 条运行，
全部由 `tmp/render-metrics.py` 从运行 CSV 直接生成，不做手工转录。
复现步骤见 `external-reproduction.md`，消融与归因见 `adaptive-module-analysis.md`。

## 1. 核心结论

下表为自适应相对各基准的 MRR 差值（自适应减去基准，负值如实列出）。
两个数据集的结论不同，因此不合并为一个“提升了多少”的数字。

| 问题 | 2WikiMultiHopQA（500 条） | HotpotQA（500 条） |
| --- | --- | --- |
| 自适应相对 BM25 单路 MRR | **+0.0499（+6.8%）** | **-0.0186（-2.3%）** |
| 自适应相对 VECTOR 单路 MRR | **+0.0713（+10.0%）** | **+0.0409（+5.3%）** |
| 自适应相对 GRAPH 单路 MRR | **+0.2607（+49.6%）** | **+0.4795（+146.5%）** |
| 自适应相对固定混合 MRR | **+0.0164（+2.1%）** | **+0.0059（+0.7%）** |
| 自适应相对固定混合 Recall@10 | +0.0060 | **-0.0190** |
| 自适应边扫描相对固定混合 | -6.16（-17.0%） | -4.76（-13.2%） |
| 是否有方法真正回答了问题 | 否，F1 最高 0.0095、EM 全 0 | 同左 |

**两数据集上的结论并不相同，如实分列：**

- **2WikiMultiHopQA**：自适应在 Recall@10（0.8495）、支持句命中率（0.8620）、MRR（0.7867）、
  NDCG@10（0.7483）上都是六种方法中最高的，同时边扫描比固定混合少 17.0%。
- **HotpotQA**：自适应 MRR 最高（0.8067），但**仍落后 BM25 单路 2.3%**；
  Recall@10 为 0.9270，落后 BM25 单路的 0.9673 与固定混合的 0.9460。
  在文本通道极强的语料上，图证据占据 topK 位置会稀释文本召回，这是尚未解决的局限。

**与修复前的同规模运行对比**：自适应 MRR 在 2Wiki 上由 0.7182 升至 0.7867（+9.5%），
在 HotpotQA 上由 0.6970 升至 0.8067（+15.7%）；两者分别由落后固定混合 6.8%、13.0%
转为领先 2.1%、0.7%。HotpotQA 的 Recall@10 同期由 0.9525 降到 0.9270，
来自融合权重统一后无锚点查询的图证据排名下降（归因见自适应分析文档第 7.2 节）。

**单路方法与固定混合在两个数据集的新旧运行中逐位相同**（已逐单元格校验），
说明提升全部来自路由配置修正，而不是语料或文本通道的变化。

## 2. 配置

| 设置 | 取值 |
| --- | --- |
| 代码提交 | `fee160853`（`codeRevision` 已写入每条记录） |
| 查询数 | 各 dev/validation 划分的前 500 条，确定性前缀 |
| `topK` / `maxCandidates` | 10 / 100 |
| 主约束 / `exactRanking` | `CANDIDATES` / true（排序前全量扫描索引） |
| 融合权重 | `graph-weight=0.25`、`text-weight=1.0`（图:文本 = 1:4） |
| 其它路由默认 | `high-average-idf=4.0`、`max-hop=1`、`decouple-text-routing=false`、`max-replan-count=1` |
| 向量 | 预计算 `gte` 向量，来自已发布 bundle |
| token 估算器 | `chars4-v1`（字符数 / 4，向上取整，下限 1） |
| 随机种子 | 0 |

## 3. 质量与命中率

**Recall@10** 是 gold 证据块在 top10 中的召回；**支持句命中率**（`supportingSentenceRecall`）
按标准答案所需的支持句计算；**全部支持句命中**（`allSupportingSentencesHit`）是多跳问题
是否**所有**支持句都被命中的比例，是三者中最严格的命中率口径。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261113T083113Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 方法 | Recall@10 | 支持句命中率 | 全部支持句命中 | MRR | NDCG@10 | F1 | EM |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.7978 | 0.8195 | 0.6820 | 0.7368 | 0.6979 | 0.00226 | 0.0000 |
| VECTOR_ONLY | 0.7142 | 0.7560 | 0.5640 | 0.7154 | 0.6406 | 0.00239 | 0.0000 |
| VECTOR_OVERFETCH | 0.7142 | 0.7560 | 0.5640 | 0.7154 | 0.6406 | 0.00239 | 0.0000 |
| GRAPH_ONLY | 0.5030 | 0.5370 | 0.3460 | 0.5260 | 0.4506 | 0.00951 | 0.0000 |
| FIXED_HYBRID | 0.8435 | 0.8615 | 0.7320 | 0.7703 | 0.7388 | 0.00242 | 0.0000 |
| ADAPTIVE_HYBRID | 0.8495 | 0.8620 | 0.7320 | 0.7867 | 0.7483 | 0.00240 | 0.0000 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261113T085612Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 方法 | Recall@10 | 支持句命中率 | 全部支持句命中 | MRR | NDCG@10 | F1 | EM |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.9673 | 0.9492 | 0.8900 | 0.8254 | 0.8235 | 0.00247 | 0.0000 |
| VECTOR_ONLY | 0.8197 | 0.8189 | 0.6760 | 0.7658 | 0.7121 | 0.00236 | 0.0000 |
| VECTOR_OVERFETCH | 0.8197 | 0.8189 | 0.6760 | 0.7658 | 0.7121 | 0.00236 | 0.0000 |
| GRAPH_ONLY | 0.3818 | 0.3812 | 0.2640 | 0.3272 | 0.3056 | 0.00793 | 0.0000 |
| FIXED_HYBRID | 0.9460 | 0.9319 | 0.8540 | 0.8008 | 0.7914 | 0.00247 | 0.0000 |
| ADAPTIVE_HYBRID | 0.9270 | 0.9104 | 0.8260 | 0.8067 | 0.7892 | 0.00247 | 0.0000 |

## 4. 自适应方案相对单路与固定混合的提升幅度

差值为自适应减去基准；**负值如实列出，不做修饰**。
`GRAPH_ONLY` 是纯图单路，`BM25_ONLY` 与 `VECTOR_ONLY` 是纯文本单路，
`FIXED_HYBRID` 是不带路由决策的固定融合。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261113T083113Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 对比基准 | Δ Recall@10 | Δ MRR | MRR 相对 | Δ NDCG@10 | Δ 边扫描 | Δ 保留 token |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 相对 BM25_ONLY | +0.0517 | +0.0499 | +6.8% | +0.0504 | +30.10 | -106.3 |
| 相对 VECTOR_ONLY | +0.1353 | +0.0713 | +10.0% | +0.1077 | +30.10 | +96.5 |
| 相对 GRAPH_ONLY | +0.3465 | +0.2607 | +49.6% | +0.2977 | -15.75 | +1857.5 |
| 相对 FIXED_HYBRID | +0.0060 | +0.0164 | +2.1% | +0.0095 | -6.16 | +22.9 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261113T085612Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 对比基准 | Δ Recall@10 | Δ MRR | MRR 相对 | Δ NDCG@10 | Δ 边扫描 | Δ 保留 token |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 相对 BM25_ONLY | -0.0403 | -0.0186 | -2.3% | -0.0343 | +33.66 | -38.0 |
| 相对 VECTOR_ONLY | +0.1073 | +0.0409 | +5.3% | +0.0771 | +33.66 | +37.1 |
| 相对 GRAPH_ONLY | +0.5452 | +0.4795 | +146.5% | +0.4836 | +13.24 | +2158.1 |
| 相对 FIXED_HYBRID | -0.0190 | +0.0059 | +0.7% | -0.0022 | -4.76 | -11.5 |

## 5. 成本

### 5.1 实测 token 与图扩展成本

`meanTokensRetained` 是真正进入答案层的证据估算 token 数（`chars4-v1`），
`meanGraphTokensEstimated` 是其中由图扩展产生的部分。
这些是**检索阶段**的数字，不是大模型计费数字。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261113T083113Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 方法 | 平均保留 token | 总保留 token | 图证据估计 token | 边扫描 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 2805.7 | 1,402,862 | 0.0 | 0.00 | 2872 | 3332 | 3616 |
| VECTOR_ONLY | 2602.9 | 1,301,437 | 0.0 | 0.00 | 960 | 1282 | 1567 |
| VECTOR_OVERFETCH | 2602.9 | 1,301,437 | 0.0 | 0.00 | 915 | 1166 | 1474 |
| GRAPH_ONLY | 841.9 | 420,933 | 906.9 | 45.85 | 1274 | 1539 | 1664 |
| FIXED_HYBRID | 2676.5 | 1,338,250 | 699.2 | 36.26 | 3011 | 3484 | 3787 |
| ADAPTIVE_HYBRID | 2699.4 | 1,349,707 | 778.7 | 30.10 | 3027 | 3497 | 3735 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261113T085612Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 方法 | 平均保留 token | 总保留 token | 图证据估计 token | 边扫描 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 2854.3 | 1,427,157 | 0.0 | 0.00 | 1773 | 2073 | 2289 |
| VECTOR_ONLY | 2779.1 | 1,389,565 | 0.0 | 0.00 | 646 | 776 | 873 |
| VECTOR_OVERFETCH | 2779.1 | 1,389,565 | 0.0 | 0.00 | 482 | 617 | 685 |
| GRAPH_ONLY | 658.2 | 329,094 | 672.7 | 20.42 | 414 | 553 | 627 |
| FIXED_HYBRID | 2827.8 | 1,413,892 | 1424.4 | 38.42 | 1860 | 2162 | 2362 |
| ADAPTIVE_HYBRID | 2816.3 | 1,408,134 | 1165.7 | 33.66 | 1872 | 2184 | 2359 |

### 5.2 若接入大模型生成答案

检索层只返回证据片段，不调用任何模型。若部署接入大模型，单次查询成本可由保留 token 推出：

```
prompt_tokens  = 系统提示 + 问题 + meanTokensRetained
单次查询成本   = prompt_tokens × 输入单价 + 输出 token × 输出单价
```

算例取**示意单价**：输入 `$0.15 / 1M` tokens、输出 `$0.60 / 1M` tokens，
系统提示 200 tokens、问题 30 tokens、回答 50 tokens。请替换为实际所用模型的当前价格；
真正重要的是方法之间的相对关系，而不是绝对金额。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261113T083113Z/2wikimultihopqa-six-methods.csv -->
| 方法 | 保留 token | 输入 token（含 200 系统提示 + 30 问题） | 每千次查询成本 |
| --- | --- | --- | --- |
| GRAPH_ONLY | 842 | 1072 | $0.19 |
| FIXED_HYBRID | 2676 | 2906 | $0.47 |
| ADAPTIVE_HYBRID | 2699 | 2929 | $0.47 |
| BM25_ONLY | 2806 | 3036 | $0.49 |

## 6. 延迟

延迟数字与机器相关，应在目标主机上重新测量。混合方法高于单通道，
主要因为 `exactRanking` 会全量扫描文本索引，而不是因为图扩展本身。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261113T083113Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 方法 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- |
| BM25_ONLY | 2872 | 3332 | 3616 |
| VECTOR_ONLY | 960 | 1282 | 1567 |
| VECTOR_OVERFETCH | 915 | 1166 | 1474 |
| GRAPH_ONLY | 1274 | 1539 | 1664 |
| FIXED_HYBRID | 3011 | 3484 | 3787 |
| ADAPTIVE_HYBRID | 3027 | 3497 | 3735 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261113T085612Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 方法 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- |
| BM25_ONLY | 1773 | 2073 | 2289 |
| VECTOR_ONLY | 646 | 776 | 873 |
| VECTOR_OVERFETCH | 482 | 617 | 685 |
| GRAPH_ONLY | 414 | 553 | 627 |
| FIXED_HYBRID | 1860 | 2162 | 2362 |
| ADAPTIVE_HYBRID | 1872 | 2184 | 2359 |

## 7. 为什么所有方法的 F1 都接近零

五种方法的 F1 都落在千分位，EM 全为 0。这反映的是**答案层**的性质，而非检索失败：

- 答案消费者是**抽取式**的：它从检索到的 chunk 中返回一个片段。
- 标准答案是短实体（`Małgorzata Braunek`、`yes`、`2`）。
- 检索到的 chunk 是实体名列表或多句段落，抽取器极少恰好输出标准答案字符串。

**召回最低的 GRAPH_ONLY 反而 F1 最高**：它窄而实体密集的证据使片段抽取更容易命中短答案。
因此在本设置下 Recall 与 F1 度量的不是同一件事，**不应用 F1 给这些方法排序**。
有意义的 F1 需要生成式答案模型，本模块不包含。

## 8. 数据来源

| 数据集 | 运行目录 | 汇总文件 |
| --- | --- | --- |
| 2WikiMultiHopQA | `tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261113T083113Z` | `2wikimultihopqa-six-methods.csv` |
| HotpotQA | `tmp/graphrag/data/runs/reproduce-hotpotqa-20261113T085612Z` | `hotpotqa-six-methods.csv` |

两次运行的 `codeRevision` 均为 `fee160853`，`tokenEstimatorVersion` 为 `chars4-v1`，
`datasetRelease` 分别为 `e9b966b931bee494`（2Wiki）与 `1f41fcdab9da26b5`（HotpotQA）。

## 9. 本报告未覆盖的范围

- **不对其它语料做质量声称。** 两个英文多跳基准无法推广到其它语言或领域。
- **不声称答案层可用。** F1 接近零说明按当前配置它不可用。
- **不做生产容量声称。** 单进程本地运行，无并发压力。
- **参数扫描只覆盖融合权重、IDF 阈值、扩散上限与跳数。** 其余阈值仍为出厂默认。
