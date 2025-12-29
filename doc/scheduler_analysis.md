# Scheduler 代码分析笔记

## 1. Scheduler 核心架构

### 1.1 调度器接口

`DistSchedule` trait 定义了调度器的核心接口：

```rust
pub trait DistSchedule: Debug + Send + Sync {
    async fn schedule(
        &self,
        node_states: &HashMap<NodeId, NodeState>,
        stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>>;
}
```

**输入参数：**
- `node_states`: 集群中所有节点的状态
- `stage_plans`: 所有 stage 的执行计划

**返回值：**
- `HashMap<TaskId, NodeId>`: Task 到节点的映射关系

### 1.2 默认调度器实现

`DefaultScheduler` 是默认的调度器实现，采用轮询策略进行任务分配。

## 2. 调度策略分类

调度器根据执行计划的特性采用两种不同的调度策略：

### 2.1 策略判断：`is_plan_fully_pipelined`

```rust
pub fn is_plan_fully_pipelined(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut fully_pipelined = true;
    plan.apply(|node| {
        let any = node.as_any();
        if any.is::<RepartitionExec>()
            || any.is::<CoalescePartitionsExec>()
            || any.is::<NestedLoopJoinExec>()
        {
            fully_pipelined = false;
        }
        if let Some(hash_join) = any.downcast_ref::<HashJoinExec>()
            && hash_join.partition_mode() == &PartitionMode::CollectLeft
        {
            fully_pipelined = false;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("plan traversal should not fail");

    fully_pipelined
}
```

**判断标准：**

**流水线化：**
- 计划中不包含以下操作符：
  - `RepartitionExec`
  - `CoalescePartitionsExec`
  - `NestedLoopJoinExec`
  - `HashJoinExec` 且模式为 `CollectLeft`

**非流水线化：**
- 包含上述任一操作符

### 2.2 策略1：流水线化 Stage - `assign_stage_tasks_to_all_nodes`

```rust
fn assign_stage_tasks_to_all_nodes(
    stage_id: StageId,
    plan: &Arc<dyn ExecutionPlan>,
    node_states: &HashMap<NodeId, NodeState>,
    task_index: &mut usize,
) -> HashMap<TaskId, NodeId> {
    let mut assignments = HashMap::new();
    let partition_count = plan.output_partitioning().partition_count();

    for partition in 0..partition_count {
        let task_id = stage_id.task_id(partition as u32);
        assignments.insert(
            task_id,
            node_states
                .keys()
                .nth(*task_index % node_states.len())
                .expect("index should be within bounds")
                .clone(),
        );
        *task_index += 1;
    }

    assignments
}
```

**特点：**
- **Task 级别轮询**：每个 task 轮询分配到不同节点
- **充分利用集群资源**：所有节点并行执行不同 task
- **适用于**：流水线化的操作，task 之间没有数据依赖

**示例：**

假设 Stage 1 有 4 个 partition，集群有 2 个节点：
- Task (job, 1, 0) → Node A
- Task (job, 1, 1) → Node B
- Task (job, 1, 2) → Node A
- Task (job, 1, 3) → Node B

### 2.3 策略2：非流水线化 Stage - `assign_stage_all_tasks_to_node`

```rust
fn assign_stage_all_tasks_to_node(
    stage_id: StageId,
    plan: &Arc<dyn ExecutionPlan>,
    node_states: &HashMap<NodeId, NodeState>,
    stage_index: &mut usize,
) -> HashMap<TaskId, NodeId> {
    let node_id = node_states
        .keys()
        .nth(*stage_index % node_states.len())
        .expect("index should be within bounds");

    let mut assignments = HashMap::new();
    let partition_count = plan.output_partitioning().partition_count();

    for partition in 0..partition_count {
        let task_id = stage_id.task_id(partition as u32);
        assignments.insert(task_id, node_id.clone());
    }

    *stage_index += 1;

    assignments
}
```

**特点：**
- **Stage 级别轮询**：整个 stage 的所有 task 分配到同一个节点
- **避免数据移动**：减少跨节点数据传输
- **适用于**：需要数据重分区或收集的操作

**示例：**

假设 Stage 2 有 4 个 partition，集群有 2 个节点：
- Task (job, 2, 0) → Node A
- Task (job, 2, 1) → Node A
- Task (job, 2, 2) → Node A
- Task (job, 2, 3) → Node A

## 3. 调度流程

```rust
async fn schedule(
    &self,
    node_states: &HashMap<NodeId, NodeState>,
    stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
) -> DistResult<HashMap<TaskId, NodeId>> {
    if node_states.is_empty() {
        return Err(DistError::schedule("No nodes available for scheduling"));
    }

    let mut assignments = HashMap::new();

    let mut stage_index = 0;
    let mut task_index = 0;

    for (stage_id, plan) in stage_plans.iter() {
        if is_plan_fully_pipelined(plan) {
            let assignment =
                assign_stage_tasks_to_all_nodes(*stage_id, plan, node_states, &mut task_index);
            assignments.extend(assignment);
        } else {
            let assignment =
                assign_stage_all_tasks_to_node(*stage_id, plan, node_states, &mut stage_index);
            assignments.extend(assignment);
            stage_index += 1;
        }
    }
    Ok(assignments)
}
```

**调度步骤：**

1. **检查节点可用性**：验证集群中是否有可用节点
2. **遍历所有 stage**：按顺序处理每个 stage
3. **判断调度策略**：根据 `is_plan_fully_pipelined` 判断选择策略
4. **流水线化 stage**：使用 task 级别轮询分配
5. **非流水线化 stage**：使用 stage 级别轮询分配

## 4. 任务分布可视化

`DisplayableTaskDistribution` 提供了任务分布的可视化展示：

```rust
impl Display for DisplayableTaskDistribution<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 按节点分组
        let mut node_tasks = HashMap::new();
        for (task_id, node_id) in self.0.iter() {
            node_tasks
                .entry(node_id)
                .or_insert_with(Vec::new)
                .push(task_id);
        }

        // 格式化输出
        let mut node_dist = Vec::new();
        for (node_id, tasks) in node_tasks
            .into_iter()
            .sorted_by_key(|(node_id, _)| *node_id)
        {
            let stage_groups = tasks.into_iter().into_group_map_by(|task_id| task_id.stage);
            let stage_groups_display = stage_groups
                .into_iter()
                .sorted_by_key(|(stage, _)| *stage)
                .map(|(stage, tasks)| {
                    format!(
                        "{stage}/{}",
                        if tasks.len() == 1 {
                            format!("{}", tasks[0].partition)
                        } else {
                            format!(
                                "{{{}}}",
                                tasks
                                    .into_iter()
                                    .sorted()
                                    .map(|t| t.partition.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                            )
                        }
                    )
                })
                .collect::<Vec<String>>()
                .join(",");
            node_dist.push(format!("{stage_groups_display}->{node_id}",));
        }

        write!(f, "{}", node_dist.join(", "))
    }
}
```

**输出格式示例：**

```
0/{0,1,2,3}->Node1, 1/0->Node1, 1/1->Node2, 1/2->Node1, 1/3->Node2
```

**含义：**
- Stage 0 的所有 partition (0,1,2,3) 都在 Node1
- Stage 1 的 partition 0,2 在 Node1，partition 1,3 在 Node2

## 5. 设计优势

1. **自适应调度**：根据执行计划特性自动选择最优调度策略
2. **负载均衡**：通过轮询机制实现节点间的负载均衡
3. **最小化数据移动**：非流水线化操作避免跨节点数据传输
4. **简单高效**：O(n) 时间复杂度，易于理解和维护

## 6. 与 Stage 设计笔记的关联

笔记中提到的关键概念在调度器中都有体现：

- **Task 数量**：由 `plan.output_partitioning().partition_count()` 决定
- **TaskId 结构**：通过 `stage_id.task_id(partition as u32)` 创建任务ID
- **Stage 依赖**：虽然调度器不直接处理依赖关系，但通过 stage_plans 的顺序间接体现了依赖顺序

## 7. 调度器缺陷分析

### 7.1 忽略节点资源差异

**问题：**
- 所有节点被视为同等，没有考虑 CPU、内存、磁盘 I/O、网络带宽等资源差异
- 可能导致资源不足的节点被分配过多任务，造成性能瓶颈
- 强大的节点资源利用不足

**改进建议：**
- 根据节点资源（CPU 核心数、内存大小、磁盘 I/O）加权分配任务
- 实现资源感知调度，如 `assign_tasks_proportional_to_resources()`

### 7.2 缺乏数据局部性考虑

**问题：**
- 没有考虑数据在哪个节点上，可能导致大量数据跨节点传输
- 对于 Scan 操作，应该优先在数据所在的节点执行

**改进建议：**
- 为 Scan 操作实现数据局部性调度
- 根据数据分布信息，将任务分配到数据所在节点
- 减少网络传输开销

### 7.3 任务执行时间差异导致的负载不均

**问题：**
- 不同任务的执行时间可能差异很大（如：Scan 大表 vs Scan 小表）
- 轮询可能导致某些节点执行耗时任务，其他节点空闲
- 没有考虑任务的复杂度和数据量

**改进建议：**
- 实现基于任务复杂度的调度
- 使用工作窃取（work stealing）机制动态平衡负载
- 根据历史执行时间预测任务耗时

### 7.4 未利用 Stage 依赖关系

**问题：**
- 调度器没有考虑 stage 之间的依赖关系（笔记中提到的 UnresolvedExec）
- 可能导致下游 stage 在上游 stage 完成前就开始分配资源
- 无法优化 stage 之间的数据传输

**改进建议：**
- 分析 stage 依赖图，实现拓扑排序调度
- 优先调度上游 stage，让下游 stage 在数据准备好后立即执行
- 考虑 stage 之间的数据传输成本

### 7.5 节点状态信息未被充分利用

**问题：**
- `NodeState` 可能包含节点负载、任务队列长度、健康状态等信息
- 当前实现完全忽略这些信息

**改进建议：**
- 根据节点当前负载动态分配任务
- 优先选择负载较低的节点
- 避免向过载节点分配新任务

### 7.6 缺乏容错和重新调度机制

**问题：**
- 如果某个节点失败，没有自动重新调度机制
- 任务执行失败后无法重新分配到其他节点
- 没有健康检查和节点隔离机制

**改进建议：**
- 实现任务重试机制
- 节点故障时自动重新调度任务
- 实现任务超时和取消机制

### 7.7 静态调度，缺乏动态调整

**问题：**
- 任务分配后无法根据执行情况动态调整
- 无法应对运行时的负载变化
- 没有反馈机制来优化后续调度决策

**改进建议：**
- 实现动态调度，根据实时执行情况调整任务分配
- 收集任务执行指标（执行时间、资源使用）
- 使用机器学习优化调度策略

### 7.8 网络拓扑未考虑

**问题：**
- 没有考虑节点之间的网络距离和带宽
- 可能将频繁通信的任务分配到网络延迟高的节点
- 跨机架、跨数据中心的通信成本未考虑

**改进建议：**
- 考虑网络拓扑，将频繁通信的任务分配到同一机架或数据中心
- 根据网络带宽和延迟优化任务分配

### 7.9 非流水线化 Stage 的调度过于简单

**问题：**
- 对于非流水线化 stage，所有 task 分配到同一个节点可能导致该节点过载
- 没有考虑该节点的资源是否足够执行所有 task
- 可能导致某些节点空闲，某些节点过载

**改进建议：**
- 根据节点的实际资源能力决定是否可以分配整个 stage
- 实现更细粒度的非流水线化调度策略
- 考虑将大 stage 拆分到多个节点

### 7.10 缺乏任务优先级机制

**问题：**
- 所有任务同等对待，没有优先级概念
- 无法区分关键路径任务和非关键路径任务
- 无法优先调度对整体性能影响最大的任务

**改进建议：**
- 实现任务优先级机制
- 识别关键路径上的任务，优先调度
- 根据任务对整体查询性能的影响分配优先级

## 8. 总结

### 8.1 适用场景

当前调度器适合：
- 小规模集群
- 同构节点（资源相同）
- 任务执行时间相近的场景

### 8.2 不适用场景

不适合：
- 大规模异构集群
- 任务执行时间差异大的场景
- 对性能要求极高的生产环境

### 8.3 改进方向

对于生产环境，需要考虑：
- 资源感知调度
- 数据局部性
- 动态负载均衡
- 容错机制
- 性能优化

建议参考成熟的分布式调度系统（如 Spark、Flink、Presto）的调度策略进行改进。
