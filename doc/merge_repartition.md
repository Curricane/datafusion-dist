# 重分区合并优化笔记

## 函数说明

`merge_adjacent_repartition_exec` 函数用于合并相邻的 `RepartitionExec` 节点，消除冗余的重分区操作，提高查询执行效率。

## 核心逻辑

1. **遍历执行计划**：使用 `transform_down` 方法向下遍历整个执行计划树
2. **识别相邻重分区**：检查当前节点是否为 `RepartitionExec`，且其子节点也是 `RepartitionExec`
3. **合并优化**：移除内层冗余的重分区节点，保留外层的分区策略

## 优化示例

```text
优化前：
RepartitionExec: partitioning=Hash([name@0, age@1], 12), input_partitions=12
  RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=4
    SomeExec

优化后：
RepartitionExec: partitioning=Hash([name@0, age@1], 12), input_partitions=4
  SomeExec
```

## 技术要点

- 保留外层节点的分区策略（如 Hash 分区）
- 直接连接到孙子节点，跳过内层冗余的重分区
- 更新输入分区数量以反映正确的数据分布

## 重分区概念

重分区是分布式计算中的数据重新分布操作，将数据从当前的分区状态重新分配到新的分区状态。

### 示例：
```text
原始分区: [分区0: 数据A, 分区1: 数据B]
重分区后: [分区0: 数据A+C, 分区1: 数据B+D]
```

## 相邻重分区出现场景

1. **查询计划优化阶段**：多个优化规则依次应用，可能产生连续的重分区
2. **算子组合**：不同算子要求不同的数据分布，导致多个重分区叠加
3. **分区策略变更**：查询执行过程中分区策略调整

## 优化原理

### 数据流向分析：
```text
原始结构：
RepartitionExec A (Hash分区, 12个输出分区)
  └── RepartitionExec B (RoundRobin分区, 12个输出分区)  
      └── SourceExec

数据流向：
SourceExec → RepartitionExec B → RepartitionExec A → 下游算子
```

### 关键洞察：

1. **分区数量匹配**：B的输出分区数(12) = A的输入分区数(12)
2. **B的分区策略是RoundRobin**：这种策略只是简单地将数据均匀分发到12个分区
3. **A是Hash分区**：A会根据Hash函数重新计算数据应该去哪个分区

### 优化后的等价性：

```text
优化前：Source → RoundRobin(12) → Hash([字段], 12) → 下游
优化后：Source → Hash([字段], 12) → 下游
```

由于Hash分区会重新计算数据分布，中间的RoundRobin步骤是冗余的：
- 数据先被RoundRobin分到12个分区
- 然后又被Hash函数重新分到12个分区
- 直接用Hash分区可以得到相同结果

### 为什么安全？

- **分区数量一致**：内层输出分区数 = 外层输入分区数
- **外层策略占主导**：最终数据分布由外层Hash分区决定
- **中间步骤冗余**：RoundRobin的重新分布被外层覆盖

这种优化减少了不必要的数据移动和网络传输，提升执行效率。