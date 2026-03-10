# GeaFlow 模型权重闭环更新落地方案

## 1. 业务背景

在 GeaFlow 与 LBS 流湖一体的架构中（见 `geaflow_lbs_integration_solution.md`），基于 PaddleSpatial 的 SAGNN 时空图模型负责在线预测路网阻抗。由于城市交通分布的“概念漂移”（Concept Drift）现象（如换季、节假日、修路导致的拥堵规律改变），静态模型权重的准确率会随时间衰减。

本方案旨在打通从 Paimon 数据湖提取特征、在独立计算集群（GPU）上进行增量训练、最终将新的 PaddlePaddle 模型权重安全、无缝地推送到 GeaFlow Infer（推理侧）进行热更新的全闭环链路。

## 2. 闭环更新架构设计

整个权重更新链路属于典型的 Lambda 架构中的“批处理/离线训练”侧：

```mermaid
graph TD
    A[(Paimon 数据湖 DWS层)] -->|1. Time Travel 批量捞取特征与标签| B[PaddlePaddle 离线训练集群]
    B -->|2. 模型增量训练与验证| C{模型效果评估 (Eval)}
    C -->|通过 (Better Metrics)| D[模型版本注册中心 / HDFS]
    C -->|未通过| E[丢弃本次权重]
    
    D -->|3. Webhook / RPC 触发更新| F[GeaFlow Infer Environment Manager]
    F -->|4. 动态分发模型文件| G[GeaFlow Worker Nodes (Python UDF)]
    G -->|5. 热加载新权重| H[SAGNNTransFormFunction]
```

## 3. 详细实施路径与核心机制

### 3.1 数据准备：基于 Paimon 的历史回溯 (Time Travel)
模型训练需要“特征 + 真实标签”。在我们的架构中：
- **特征**：过去某时刻的网点属性、邻居特征（从 Paimon 的 `traffic_history` 结合图快照拉取）。
- **标签**：该时刻之后真实发生的路况阻抗（也已沉淀在 Paimon 中）。

由于 Paimon 支持 Time Travel，离线训练集群可以使用 PySpark 或 Flink 提交批任务，构建出严格对齐的历史特征切片：
```python
# 示例：通过 PySpark 读取 Paimon 数据构建训练集
df = spark.read.format("paimon") \
    .option("scan.mode", "from-timestamp") \
    .option("scan.timestamp-millis", "1710000000000") \
    .load("hdfs://.../xiamen_logistics/traffic_history")
```

### 3.2 离线增量训练 (Incremental Training)
在独立的 GPU 训练集群上，加载现有的 `sagnn_model.pdparams`，将新的 Paimon 特征集喂入模型进行微调（Fine-tuning）。
- **学习率策略**：使用极小的学习率（如 $1e-5$），以防止“灾难性遗忘”（Catastrophic Forgetting）。
- **离线验证 (Eval)**：在一组 Hold-out 的测试集上评估 MSE 或 MAE。只有当新权重的准确率高于现网运行版本时，才进入发布环节。
- **权重导出**：将验收通过的模型保存为新的 `.pdparams` 文件，并附带时间戳或版本号上传至对象存储（如 HDFS、S3 或专用的 MLflow 注册中心）。

### 3.3 GeaFlow Infer 侧的权重热加载 (Hot Reloading)
这是工程上最具挑战性的一环。GeaFlow Infer 框架中的 Python 进程是由 Java Worker 节点 fork 出来的，且通过 Socket/Shared Memory 通信。重启整个 GeaFlow 图计算作业成本极高。我们需要实现**动态热更新**。

**改造现有的 `SAGNNTransFormFunction`：**

在现有的 `PaddleSpatialSAGNNTransFormFunctionUDF.py` 中，增加定时轮询或监听机制，监测模型文件是否发生变化。

```python
import os
import time
import paddle
from typing import Tuple, List

class SAGNNTransFormFunction(TransFormFunction):
    def __init__(self):
        super().__init__(input_size=3)
        self.model_path = os.path.join(os.getcwd(), "sagnn_model.pdparams")
        self.last_load_time = 0
        self.update_interval = 3600  # 每小时检查一次模型更新
        
        # 初始化模型架构
        self._init_model_architecture()
        self._check_and_load_weights()

    def _init_model_architecture(self):
        self.model = SAGNNModel(...)
        self.model.eval()

    def _check_and_load_weights(self):
        """检查模型文件的时间戳，若有更新则热加载"""
        if os.path.exists(self.model_path):
            current_mtime = os.path.getmtime(self.model_path)
            if current_mtime > self.last_load_time:
                try:
                    # 使用 paddle.load 热替换 state_dict
                    state_dict = paddle.load(self.model_path)
                    self.model.set_state_dict(state_dict)
                    self.last_load_time = current_mtime
                    print(f"[SAGNN UDF] Successfully hot-reloaded new weights from {self.model_path}")
                except Exception as e:
                    print(f"[SAGNN UDF] Failed to hot-reload weights: {e}")

    def transform_pre(self, *args) -> Tuple[List[float], object]:
        # 1. 在每次推理前（或按时间窗口）检查模型是否有更新
        if time.time() - getattr(self, '_last_check_time', 0) > self.update_interval:
            self._check_and_load_weights()
            self._last_check_time = time.time()
            
        # 2. 正常的推理逻辑
        ...
```

### 3.4 模型分发与一致性保障
为了配合上述 Python 侧的热加载，我们需要一个外部机制将新的模型文件推送到 GeaFlow 各个 Worker 的工作目录。

- **方案 A (简单实现：NFS/HDFS 挂载)**：将包含 `sagnn_model.pdparams` 的目录挂载为所有 Worker 都能访问的网络文件系统。训练集群直接覆盖此文件。GeaFlow Worker 内的 Python UDF 会自动感知文件修改时间并重载。
- **方案 B (高级实现：GeaFlow API 触发)**：开发一个轻量级的 Sidecar 服务部署在 Worker 节点上。离线集群训练完毕后，调用 Sidecar 的 HTTP 接口，Sidecar 负责从 HDFS 下载新权重并原子性地替换工作目录下的旧权重（先下载为 `.tmp`，再 `mv` 覆盖），确保加载过程中不发生文件损坏。

## 4. 生产级验收清单 (Checklist)
1. **原子性覆盖**：替换模型文件必须使用操作系统的原子性操作（如 Linux `mv`），严禁边写边读，防止 `paddle.load` 读取到损坏的文件。
2. **预热 (Warmup)**：新模型加载后，首次推理（JIT 编译期）可能会有延迟毛刺，需在 `load` 后用一条 dummy data 进行预热。
3. **版本回退 (Rollback)**：在 Worker 工作目录至少保留上一个版本的权重 `sagnn_model.pdparams.bak`。如果热加载引发 Python 侧 OOM 或推理异常崩溃，UDF 的 `except` 块应能自动回退到备份权重。
4. **监控打点**：将模型的版本号（或 Load Time）作为一条监控 Metric 打出，确保整个集群所有 Worker 都成功更新到了最新版本，避免“版本脑裂”。