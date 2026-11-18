# GraphRAG 检索：性能与成本指标报告

对应维护者评审的第 2 点：把**成本与效果放在一起**汇报，补充 token 成本、命中率与 F1，
并给出自适应方案相对每一种单路方案的**具体**提升幅度——包括提升有限甚至为负的情形。

**本报告数字由当前代码从零重建即可复现。** 两个数据集均使用按当前代码重新构建的 bundle
（`build_code_revision=bba26e94f8f4233566a1a79b9563bae898a7b304`），
而非更早生成的历史 bundle；重复构建得到相同的 bundle 版本与边数，验证了其确定性。
所有表格由 `tmp/render-metrics.py` 从运行 CSV 直接生成，不做手工转录。
复现步骤见 `external-reproduction.md`，消融与归因见 `adaptive-module-analysis.md`。

## 1. 核心结论

| 问题 | 2WikiMultiHopQA（500 条） | HotpotQA（500 条） |
| --- | --- | --- |
| 自适应相对 BM25 单路 MRR | **+0.0482（+6.5%）** | **-0.0158（-1.9%）** |
| 自适应相对 VECTOR 单路 MRR | **+0.0696（+9.7%）** | **+0.0441（+5.8%）** |
| 自适应相对 GRAPH 单路 MRR | **+0.2713（+52.8%）** | **+0.4826（+147.6%）** |
| 自适应相对固定混合 MRR | **+0.0179（+2.3%）** | **+0.0092（+1.2%）** |
| 自适应相对固定混合 Recall@10 | +0.0070 | -0.0210 |
| 是否有方法真正回答了问题 | 否，F1 最高 0.0091、EM 全 0 | 同左 |

**两数据集上的结论并不相同，如实分列：**

- **2WikiMultiHopQA**：自适应在 Recall@10（0.8485）、MRR（0.7850）、NDCG@10（0.7478）上
  均为六种方法中最高；支持句命中率 0.8600、全部支持句命中 0.7280，
  略低于固定混合的 0.8635 与 0.7360。
- **HotpotQA**：自适应 MRR 最高（0.8096），但**仍落后 BM25 单路 1.9%**；
  Recall@10 为 0.9260，落后 BM25 单路的 0.9673 与固定混合的 0.9470。
  在文本通道极强的语料上，图证据占据 topK 位置会稀释文本召回，这是尚未解决的局限。

**与使用历史 bundle 的旧报告相比**：自适应 MRR 在 2Wiki 上为 0.7867 → 0.7850、
HotpotQA 上为 0.8067 → 0.8096，差异均在 1% 以内，结论方向完全一致。
历史 bundle 的图边数比当前构建少（2Wiki 640,275 对 834,797），
但对最终混合排序的影响很小；单路方法在两次运行中仍然逐位相同。

## 2. 配置

| 设置 | 取值 |
| --- | --- |
| 代码提交 | `bba26e94f8f4233566a1a79b9563bae898a7b304`（`codeRevision` 已写入每条记录） |
| bundle | `2wikimultihopqa-bundle-current`（版本 `76e9e605acf9be79`）、`hotpotqa-bundle-current`（版本 `1341e9e53ee11765`） |
| bundle 构建代码修订 | `bba26e94f8f4233566a1a79b9563bae898a7b304`（`READY.json` 中的 `build_code_revision`） |
| 查询数 | 各 dev/validation 划分的前 500 条，确定性前缀 |
| `topK` / `maxCandidates` | 10 / 100 |
| 主约束 / `exactRanking` | `CANDIDATES` / true（排序前全量扫描索引） |
| 融合权重 | `graph-weight=0.25`、`text-weight=1.0`（图:文本 = 1:4） |
| 其它路由默认 | `high-average-idf=4.0`、`max-hop=1`、`decouple-text-routing=false`、`max-replan-count=1` |
| 向量 | 预计算 `gte` 向量，模型 `iic/nlp_gte_sentence-embedding_english-small` |
| token 估算器 | `chars4-v1`（字符数 / 4，向上取整，下限 1） |
| 随机种子 | 0 |

## 3. 质量与命中率

**Recall@10** 是 gold 证据块在 top10 中的召回；**支持句命中率**（`supportingSentenceRecall`）
按标准答案所需的支持句计算；**全部支持句命中**（`allSupportingSentencesHit`）是多跳问题
是否**所有**支持句都被命中的比例，是三者中最严格的命中率口径。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261116T093901Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 方法 | Recall@10 | 支持句命中率 | 全部支持句命中 | MRR | NDCG@10 | F1 | EM |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.7978 | 0.8195 | 0.6820 | 0.7368 | 0.6979 | 0.00226 | 0.0000 |
| VECTOR_ONLY | 0.7142 | 0.7560 | 0.5640 | 0.7154 | 0.6406 | 0.00239 | 0.0000 |
| VECTOR_OVERFETCH | 0.7142 | 0.7560 | 0.5640 | 0.7154 | 0.6406 | 0.00239 | 0.0000 |
| GRAPH_ONLY | 0.4978 | 0.5328 | 0.3440 | 0.5136 | 0.4435 | 0.00906 | 0.0000 |
| FIXED_HYBRID | 0.8415 | 0.8635 | 0.7360 | 0.7671 | 0.7367 | 0.00241 | 0.0000 |
| ADAPTIVE_HYBRID | 0.8485 | 0.8600 | 0.7280 | 0.7850 | 0.7478 | 0.00240 | 0.0000 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261116T100300Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 方法 | Recall@10 | 支持句命中率 | 全部支持句命中 | MRR | NDCG@10 | F1 | EM |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 0.9673 | 0.9492 | 0.8900 | 0.8254 | 0.8235 | 0.00247 | 0.0000 |
| VECTOR_ONLY | 0.8197 | 0.8189 | 0.6760 | 0.7655 | 0.7119 | 0.00236 | 0.0000 |
| VECTOR_OVERFETCH | 0.8197 | 0.8189 | 0.6760 | 0.7655 | 0.7119 | 0.00236 | 0.0000 |
| GRAPH_ONLY | 0.3822 | 0.3808 | 0.2660 | 0.3270 | 0.3056 | 0.00801 | 0.0000 |
| FIXED_HYBRID | 0.9470 | 0.9319 | 0.8540 | 0.8003 | 0.7914 | 0.00247 | 0.0000 |
| ADAPTIVE_HYBRID | 0.9260 | 0.9094 | 0.8240 | 0.8096 | 0.7895 | 0.00246 | 0.0000 |

### 路径命中率（path-hit）

除证据级召回外，还报告**关系路径命中**：检索返回的图路径是否覆盖 gold 推理链。
`relationPathTargetCount` 是每查询的关系目标数（2Wiki 取 `evidences` 显式标注，
HotpotQA 以 supporting-fact 标题作为关系信号）；`relationPathHit` 为命中比例。

| 方法 | 2Wiki path-hit | 2Wiki 含路径的查询 | HotpotQA path-hit | HotpotQA 含路径的查询 |
| --- | --- | --- | --- | --- |
| BM25_ONLY / VECTOR_ONLY / VECTOR_OVERFETCH | 0.0000 | 0 / 500 | 0.0000 | 0 / 500 |
| GRAPH_ONLY | **0.0060** | 495 / 500 | 0.0000 | 359 / 500 |
| FIXED_HYBRID | 0.0040 | 385 / 500 | 0.0000 | 418 / 500 |
| ADAPTIVE_HYBRID | 0.0000 | 397 / 500 | 0.0000 | 426 / 500 |

**路径命中率接近零，必须与证据召回分开解读。** 图方法确实返回了路径
（2Wiki 上 495/500 查询含路径），但这些路径几乎不覆盖 gold 推理链。
图通道当前提供的是**实体邻域证据**，而不是沿推理链的关系遍历。
自适应在 2Wiki 上 path-hit 为 0，低于 graph-only 的 0.006：
它减少了图扩展并降低了图排名权重，因此路径层面的贡献进一步减少。

**根因是图构建方式，而不是路由调参。** 当前图由 `text-cooccurrence-entity-v2` 构建，
边表示“两个实体在同一段落中共现”，**不包含 gold 标注所要求的有向语义关系**。
因此扩展得到的是实体邻域，gold 推理链（如 `A --导演--> B --主演--> C`）在图中根本不存在，
任何路由或融合权重都无法让 path-hit 变为非零。

要提升该指标，必须改图构建或图检索的语义：在摄取时引入**关系抽取**
（有向边、关系类型、与 gold 关系对齐的本体），或在扩展时施加**关系约束**
（按 gold 关系类型做定向游走、路径长度与类型联合约束）。
这属于图构建层的改造，需要单独一轮实验；本报告把它列为开放问题，
而不是把它当作检索质量的整体否定——证据级召回与排序指标是独立测量的。

## 4. 自适应方案相对单路与固定混合的提升幅度

差值为自适应减去基准；**负值如实列出，不做修饰**。
`GRAPH_ONLY` 是纯图单路，`BM25_ONLY` 与 `VECTOR_ONLY` 是纯文本单路，
`FIXED_HYBRID` 是不带路由决策的固定融合。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261116T093901Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 对比基准 | Δ Recall@10 | Δ MRR | MRR 相对 | Δ NDCG@10 | Δ 边扫描 | Δ 保留 token |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 相对 BM25_ONLY | +0.0507 | +0.0482 | +6.5% | +0.0499 | +30.13 | -106.3 |
| 相对 VECTOR_ONLY | +0.1343 | +0.0696 | +9.7% | +0.1073 | +30.13 | +96.6 |
| 相对 GRAPH_ONLY | +0.3507 | +0.2713 | +52.8% | +0.3044 | -15.92 | +1855.8 |
| 相对 FIXED_HYBRID | +0.0070 | +0.0179 | +2.3% | +0.0112 | -6.48 | +23.1 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261116T100300Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 对比基准 | Δ Recall@10 | Δ MRR | MRR 相对 | Δ NDCG@10 | Δ 边扫描 | Δ 保留 token |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 相对 BM25_ONLY | -0.0413 | -0.0158 | -1.9% | -0.0339 | +33.47 | -38.7 |
| 相对 VECTOR_ONLY | +0.1063 | +0.0441 | +5.8% | +0.0776 | +33.47 | +36.5 |
| 相对 GRAPH_ONLY | +0.5438 | +0.4826 | +147.6% | +0.4839 | +12.95 | +2159.3 |
| 相对 FIXED_HYBRID | -0.0210 | +0.0092 | +1.2% | -0.0019 | -5.04 | -11.6 |

## 5. 成本

### 5.1 实测 token 与图扩展成本

`meanTokensRetained` 是真正进入答案层的证据估算 token 数（`chars4-v1`），
`meanGraphTokensEstimated` 是其中由图扩展产生的部分。
这些是**检索阶段**的数字，不是大模型计费数字。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261116T093901Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 方法 | 平均保留 token | 总保留 token | 图证据估计 token | 边扫描 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 2805.7 | 1,402,862 | 0.0 | 0.00 | 2865 | 3288 | 3556 |
| VECTOR_ONLY | 2602.8 | 1,301,403 | 0.0 | 0.00 | 964 | 1299 | 1580 |
| VECTOR_OVERFETCH | 2602.8 | 1,301,403 | 0.0 | 0.00 | 927 | 1148 | 1403 |
| GRAPH_ONLY | 843.6 | 421,799 | 908.7 | 46.05 | 1282 | 1474 | 1592 |
| FIXED_HYBRID | 2676.3 | 1,338,175 | 702.1 | 36.61 | 2999 | 3371 | 3640 |
| ADAPTIVE_HYBRID | 2699.4 | 1,349,719 | 781.9 | 30.13 | 3012 | 3417 | 3649 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261116T100300Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 方法 | 平均保留 token | 总保留 token | 图证据估计 token | 边扫描 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BM25_ONLY | 2854.3 | 1,427,157 | 0.0 | 0.00 | 1750 | 1950 | 2031 |
| VECTOR_ONLY | 2779.1 | 1,389,533 | 0.0 | 0.00 | 636 | 714 | 750 |
| VECTOR_OVERFETCH | 2779.1 | 1,389,533 | 0.0 | 0.00 | 463 | 562 | 597 |
| GRAPH_ONLY | 656.3 | 328,153 | 670.8 | 20.52 | 405 | 510 | 576 |
| FIXED_HYBRID | 2827.2 | 1,413,600 | 1423.1 | 38.51 | 1815 | 1999 | 2101 |
| ADAPTIVE_HYBRID | 2815.6 | 1,407,798 | 1163.5 | 33.47 | 1846 | 2033 | 2104 |

### 5.2 若接入大模型生成答案

检索层只返回证据片段，不调用任何模型。若部署接入大模型，单次查询成本可由保留 token 推出：

```
prompt_tokens  = 系统提示 + 问题 + meanTokensRetained
单次查询成本   = prompt_tokens × 输入单价 + 输出 token × 输出单价
```

算例取**示意单价**：输入 `$0.15 / 1M` tokens、输出 `$0.60 / 1M` tokens，
系统提示 200 tokens、问题 30 tokens、回答 50 tokens。请替换为实际所用模型的当前价格；
真正重要的是方法之间的相对关系，而不是绝对金额。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261116T093901Z/2wikimultihopqa-six-methods.csv -->
| 方法 | 保留 token | 输入 token（含 200 系统提示 + 30 问题） | 每千次查询成本 |
| --- | --- | --- | --- |
| GRAPH_ONLY | 844 | 1074 | $0.19 |
| FIXED_HYBRID | 2676 | 2906 | $0.47 |
| ADAPTIVE_HYBRID | 2699 | 2929 | $0.47 |
| BM25_ONLY | 2806 | 3036 | $0.49 |

## 6. 延迟

延迟数字与机器相关，应在目标主机上重新测量。混合方法高于单通道，
主要因为 `exactRanking` 会全量扫描文本索引，而不是因为图扩展本身。

<!-- source: tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261116T093901Z/2wikimultihopqa-six-methods.csv -->
### 2WikiMultiHopQA，500 条查询

| 方法 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- |
| BM25_ONLY | 2865 | 3288 | 3556 |
| VECTOR_ONLY | 964 | 1299 | 1580 |
| VECTOR_OVERFETCH | 927 | 1148 | 1403 |
| GRAPH_ONLY | 1282 | 1474 | 1592 |
| FIXED_HYBRID | 2999 | 3371 | 3640 |
| ADAPTIVE_HYBRID | 3012 | 3417 | 3649 |

<!-- source: tmp/graphrag/data/runs/reproduce-hotpotqa-20261116T100300Z/hotpotqa-six-methods.csv -->
### HotpotQA distractor，500 条查询

| 方法 | P50 毫秒 | P95 毫秒 | P99 毫秒 |
| --- | --- | --- | --- |
| BM25_ONLY | 1750 | 1950 | 2031 |
| VECTOR_ONLY | 636 | 714 | 750 |
| VECTOR_OVERFETCH | 463 | 562 | 597 |
| GRAPH_ONLY | 405 | 510 | 576 |
| FIXED_HYBRID | 1815 | 1999 | 2101 |
| ADAPTIVE_HYBRID | 1846 | 2033 | 2104 |

## 7. 为什么所有方法的 F1 都接近零

五种方法的 F1 都落在千分位，EM 全为 0。这反映的是**答案层**的性质，而非检索失败：

- 答案消费者是**抽取式**的：它从检索到的 chunk 中返回一个片段。
- 标准答案是短实体（`Małgorzata Braunek`、`yes`、`2`）。
- 检索到的 chunk 是实体名列表或多句段落，抽取器极少恰好输出标准答案字符串。

**召回最低的 GRAPH_ONLY 反而 F1 最高**（2Wiki 0.0091，HotpotQA 0.0080）：
它窄而实体密集的证据使片段抽取更容易命中短答案。
因此在本设置下 Recall 与 F1 度量的不是同一件事，**不应用 F1 给这些方法排序**。
有意义的 F1 需要生成式答案模型，本模块不包含。

## 8. 数据来源

| 数据集 | bundle（当前代码构建） | 运行目录 | 汇总文件 |
| --- | --- | --- | --- |
| 2WikiMultiHopQA | `2wikimultihopqa-bundle-current`（`76e9e605acf9be79`） | `tmp/graphrag/data/runs/reproduce-2wikimultihopqa-20261116T093901Z` | `2wikimultihopqa-six-methods.csv` |
| HotpotQA | `hotpotqa-bundle-current`（`1341e9e53ee11765`） | `tmp/graphrag/data/runs/reproduce-hotpotqa-20261116T100300Z` | `hotpotqa-six-methods.csv` |

两次运行的 `codeRevision` 均为 `bba26e94f`，`tokenEstimatorVersion` 为 `chars4-v1`，
`datasetRelease` 分别为 `e9b966b931bee494`（2Wiki）与 `1f41fcdab9da26b5`（HotpotQA）。
两个 bundle 的 `build_code_revision` 也均为 `bba26e94f`，重复构建可得到相同版本与边数。

## 9. 本报告未覆盖的范围

- **不对其它语料做质量声称。** 两个英文多跳基准无法推广到其它语言或领域。
- **不声称答案层可用。** F1 接近零说明按当前配置它不可用。
- **不做生产容量声称。** 单进程本地运行，无并发压力。
- **参数扫描只覆盖融合权重、IDF 阈值、扩散上限与跳数。** 其余阈值仍为出厂默认。
