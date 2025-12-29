# CASTS 推理机 API 详解

本文档旨在深入剖析 CASTS 在**每一步决策**时的内部工作流，聚焦于其核心——推理机。

## 核心组件与依赖

推理机由三个**内部组件**和两个**外部服务**协同工作，共同完成决策。

### 内部核心组件

1.  **`StrategyCache` (策略缓存)**：作为决策的“一线员工”，它快速、廉价地处理绝大多数请求。
2.  **`LLMOracle` (LLM预言机)**：作为“专家顾问”，在缓存“没主意”时提供深度分析和最终决策。
3.  **图引擎 (Graph Engine)**：决策的**执行者**。它接收来自推理机的指令（如下一步的遍历语句），并将其应用在图上，返回执行结果。

### 依赖的外部服务

| 服务 | 描述 |
| :--- | :--- |
| **LLM 服务** | `LLMOracle` 依赖此服务进行深度推理。核心的智能来源于此。 |
| **嵌入服务 (`EmbeddingService`)** | 该服务将节点的属性转化为向量（“嵌入”），供 `StrategyCache` 在 Tier 2 匹配时进行相似度搜索。 |

---

## 推理工作流

### 1. 推理机输入：决策上下文 (`Context`)

在每个决策点，推理机接收的输入是**决策上下文 (`Context`)**，它整合了来自多个源头的信息：

| 输入类别 | 具体内容 | 来源 | 作用 |
| :--- | :--- | :--- | :--- |
| **核心上下文** | `structural_signature` (s), `properties` (p), `goal` (g) | `SimulationEngine` | 描述“我们从哪来、在哪、要去哪”的核心三要素。 |
| **状态机约束** | `next_step_options` | `GremlinStateMachine` | 限制了下一步**可以做什么类型的操作**（例如，在节点上可以 `out`, 但在边上只能 `inV`）。 |
| **图模式约束** | `valid_labels` | `GraphSchema` | 提供了**具体可用的路径**。例如，即使LLM想走 `out('friend')`，但如果当前节点没有 `friend` 类型的出边，这个选项也会被排除。 |

> **关于 `structural_signature`**
> 它不包含具体的节点 ID，而是对路径的“形状”进行描述。例如，一条具体的遍历路径可能是 `g.V('123').outE('knows').inV()`，它对应的 `structural_signature` 就是 `"V().outE().inV()"`。

### 2. 推理机内部状态：策略知识库 (Cache)

推理机的“记忆”就是 `StrategyCache` 中存储的**策略知识单元（SKU）** 列表。每个 SKU 都是一条“经验法则”，是过去 LLM 成功决策的浓缩和泛化。

| SKU 字段 | 对应数学模型 | 描述 |
| :--- | :--- | :--- |
| `id` | - | 唯一标识符。 |
| `structural_signature` | $s_{\text{sku}}$ | 该规则适用的路径结构。 |
| `predicate` | $\Phi(p)$ | 一个Python `lambda` 函数，定义了规则生效的属性条件。 |
| `goal_template` | $g_{\text{sku}}$ | 该规则适用的任务目标。 |
| `decision_template` | $d_{\text{template}}$ | 预定义的下一步决策，如 `out('knows')`。 |
| `property_vector` | $v_{\text{proto}}$ | 生成此 SKU 时节点属性的嵌入向量，用于相似度匹配。 |
| `confidence_score` | $\eta$ | 基于历史表现的动态置信度分数。 |
| `logic_complexity` | $\sigma_{\text{logic}}$ | 谓词的复杂度，用于调整相似度匹配的阈值。 |

### 3. 推理过程：决策、降级与学习（补充材料，方便理解，和 API 定义无关）

当接收到输入后，推理机按以下顺序执行决策：

1.  **Tier 1: 逻辑匹配 (最高效)**
    * **动作**: 查找知识库中是否有 SKU 的 `structural_signature`、`goal_template` 与当前上下文完全匹配，并且其 `predicate` 函数对当前节点属性 `p` 返回 `True`。
    * **输出 (命中)**: 如果找到，直接返回该 SKU 的 `decision_template` 作为决策。
    * **输出 (未命中)**: 如果未找到，进入 Tier 2。

2.  **Tier 2: 相似度匹配**
    * **动作**: 筛选出 `structural_signature` 和 `goal_template` 匹配的 SKU，然后使用 **`EmbeddingService`** 计算当前属性 `p` 的向量与这些 SKU 的 `property_vector` 之间的余弦相似度。
    * **输出 (命中)**: 如果找到一个相似度足够高（高于动态阈值 $\delta_{\text{sim}}$）的 SKU，则返回其 `decision_template`。
    * **输出 (未命中)**: 如果仍然未找到，进入最终降级。

3.  **最终降级: 求助 LLM 预言机 (最昂贵)**
    * **动作**: `StrategyCache` 返回“不知道” (`None`)。上层引擎捕获到这个信号后，将完整的输入打包，发送给 `LLMOracle`。
    * **LLM 推理**: `LLMOracle` 调用其依赖的 **LLM 服务**，根据精心设计的 Prompt 进行一次完整的推理。
    * **输出 (权威决策)**: LLM 返回一个它认为最佳的决策。
    * **学习新知识**: `LLMOracle` 将这次昂贵的推理结果“固化”，生成一个**全新的 SKU**，并将其存入 `StrategyCache`。

这个 **“尝试缓存 -> 失败则求助 -> 学习并反哺缓存”** 的闭环，是 CASTS 系统的核心学习机制。
