# DataFusion-Dist Stage 设计笔记

## 1. Stage 依赖关系的本质

### 核心概念
- **Stage 的依赖关系是通过 ExecutionPlan 的树形结构隐式表达的，而不是显式存储的**
- **依赖关系 = 执行计划树中的 UnresolvedExec 节点**
- Stage 0 是最终输出阶段，其他所有 stage 都必须被某个 stage 依赖
- 中间 stage（Stage 1, Stage 2, ...）之间通常是并行的，没有依赖关系

### 示例
假设有一个查询计划：
```
HashJoin (Stage 0)
├── Scan TableA (Stage 1)
└── Scan TableB (Stage 2)
```

拆分后：
- `stage_plans[Stage 0]` = HashJoinExec，其 children 为：
  - UnresolvedExec(delegated_stage_id=Stage 1)
  - UnresolvedExec(delegated_stage_id=Stage 2)
- `stage_plans[Stage 1]` = Scan TableA
- `stage_plans[Stage 2]` = Scan TableB

**Stage 0 依赖 Stage 1 和 Stage 2，因为它的计划中包含指向这两个 stage 的 UnresolvedExec。**

## 2. Stage 拆分过程

### plan_stages 方法
```rust
// 从底向上遍历执行计划树
let final_plan = plan
    .transform_up(|node| {
        if is_plan_children_can_be_stages(node.as_ref()) {
            let mut new_children = Vec::with_capacity(node.children().len());

            for child in node.children() {
                let stage_id = StageId {
                    job_id,
                    stage: stage_count,
                };
                stage_plans.insert(stage_id, child.clone());  // 保存子计划为独立 stage
                stage_count -= 1;

                let new_child = UnresolvedExec::new(stage_id, child.clone());
                new_children.push(Arc::new(new_child) as Arc<dyn ExecutionPlan>);
            }
            let new_plan = node.with_new_children(new_children)?;
            Ok(Transformed::yes(new_plan))
        } else {
            Ok(Transformed::no(node))
        }
    })?
    .data;
```

### 拆分步骤
1. 从底向上遍历执行计划树
2. 当遇到可分阶段的节点（HashJoin、Partial Aggregate）时
3. 将其子节点替换为 `UnresolvedExec`，并保存原始子计划到 `stage_plans`
4. `UnresolvedExec` 记录了 `delegated_stage_id`，这就是依赖关系的核心

### 可分阶段的操作
- `HashJoinExec` with `PartitionMode::Partitioned`
- `AggregateExec` with `AggregateMode::Partial`

## 3. Task 切分机制

### Task 的数量
**Task 的数量由 Stage 执行计划的 output_partitioning 决定**

```rust
let partition_count = unresolved
    .delegated_plan
    .output_partitioning()
    .partition_count();
```

### TaskId 结构
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub job_id: Uuid,
    pub stage: u32,
    pub partition: u32,
}
```

### Task 切分规则
- 每个 Stage 的 task 数量 = 该 stage 执行计划的 `output_partitioning().partition_count()`
- TaskId = (job_id, stage, partition)
- 每个 partition 对应一个独立的 task，可以分配到不同节点执行

## 4. 依赖关系的实际使用

### 执行流程
1. Stage 0 的某个 partition 执行时
2. 遇到 UnresolvedExec 节点（指向 Stage 1）
3. 转换为 ProxyExec
4. ProxyExec.execute() 被调用
5. 根据任务分布，调用 `execute_local` 或 `execute_remote` 获取 Stage 1 的结果
6. 这些结果作为 Stage 0 执行的输入

### ProxyExec 执行
```rust
fn execute(
    &self,
    partition: usize,
    _context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let task_id = self.delegated_stage_id.task_id(partition as u32);
    let node_id = self.delegated_task_distribution.get(&task_id)?;

    let fut = get_df_batch_stream(
        self.runtime.clone(),
        node_id.clone(),
        task_id,
        schema,
    );
    let stream = futures::stream::once(fut).try_flatten();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
```

## 5. 如何知道依赖关系

### 遍历 stage 的计划树
```rust
// 遍历某个 stage 的计划树，收集所有 UnresolvedExec 的 delegated_stage_id
plan.apply(|node| {
    if let Some(unresolved) = node.as_any().downcast_ref::<UnresolvedExec>() {
        depended_stages.insert(unresolved.delegated_stage_id);
    }
    Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
})?;
```

### check_initial_stage_plans 验证
```rust
// 收集所有被依赖的 stage ID
let mut depended_stages: HashSet<StageId> = HashSet::new();

for (_, plan) in stage_plans.iter() {
    plan.apply(|node| {
        if let Some(unresolved) = node.as_any().downcast_ref::<UnresolvedExec>() {
            depended_stages.insert(unresolved.delegated_stage_id);
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;
}

// 检查除 stage 0 外的每个 stage 都被其他 stage 依赖
for stage_id in stage_plans.keys() {
    if stage_id.stage != 0 && !depended_stages.contains(stage_id) {
        return Err(DistError::internal(format!(
            "Stage {} is not depended upon by any other stage",
            stage_id.stage
        )));
    }
}
```

## 6. 设计优势

### 灵活性
- 可以处理任意深度的 stage 依赖
- 支持复杂的执行计划树结构

### 并行性
- 同一层级的 stage 可以并行执行
- Task 级别的并行执行

### 简单性
- 不需要维护显式的依赖图
- 依赖关系通过计划树自然表达
- 利用 DataFusion 的 ExecutionPlan 树形结构

## 7. 关键数据结构

### StageId
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StageId {
    pub job_id: Uuid,
    pub stage: u32,
}
```

### UnresolvedExec
```rust
pub struct UnresolvedExec {
    pub delegated_stage_id: StageId,
    pub delegated_plan: Arc<dyn ExecutionPlan>,
}
```

### ProxyExec
```rust
pub struct ProxyExec {
    pub delegated_stage_id: StageId,
    pub delegated_plan_name: String,
    pub delegated_plan_properties: PlanProperties,
    pub delegated_task_distribution: HashMap<TaskId, NodeId>,
    pub runtime: DistRuntime,
}
```

## 8. Stage 生命周期

### 创建阶段
1. `plan_stages` 生成 `HashMap<StageId, Arc<dyn ExecutionPlan>>`
2. `check_initial_stage_plans` 验证 stage 计划的完整性
3. `resolve_stage_plan` 将 UnresolvedExec 转换为 ProxyExec

### 执行阶段
1. `schedule` 分配 task 到各个节点
2. `send_tasks` 发送 stage 计划和任务到集群节点
3. `execute_local`/`execute_remote` 执行具体 task

### 清理阶段
1. TaskStream Drop 时发送 TaskCompleted 事件
2. Stage 完成时触发 CheckJobCompleted 事件
3. 作业完成后清理所有相关 stage
