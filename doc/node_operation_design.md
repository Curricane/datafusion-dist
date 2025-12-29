# NodeOperation 设计文档

## 概述

本文档描述了分布式系统中节点操作的设计方案，用于统一管理集群节点的各种监控和操作功能。

## 设计原则

1. **参数绑定**：每个操作类型都携带它所需的参数
2. **统一接口**：所有操作都使用流式返回，简化接口设计
3. **类型安全**：编译时确保传递正确的参数给正确的操作
4. **扩展性**：易于添加新的操作类型和参数

## 数据结构定义

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum NodeOperation {
    GetRunningJobs {
        limit: Option<u32>,  // 可选的限制数量
        offset: Option<u32>, // 可选的偏移量
    },
    GetReadyJobs {
        limit: Option<u32>,
        offset: Option<u32>,
    }, 
    GetCpuUsage {
        sample_interval_ms: Option<u64>,  // 可选的采样间隔
        sample_count: Option<u32>,        // 可选的采样次数
    },
    GetNodeInfo,  // 不需要额外参数
    GetAllMetrics {
        include_details: bool,  // 是否包含详细信息
    },
    GetJobDetails {
        job_id: String,  // 获取特定job的详细信息
    },
    GetMemoryUsage {
        detail_level: MemoryDetailLevel,  // 内存使用的详细程度
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MemoryDetailLevel {
    Basic,    // 基本内存使用情况
    Detailed, // 详细的内存分配信息
}

// 统一的流式响应项
#[derive(Debug, Clone)]
pub struct NodeOperateStreamItem {
    pub node_id: NodeId,
    pub operation: NodeOperation,
    pub result: StreamResult,
}

#[derive(Debug, Clone)]
pub enum StreamResult {
    // Job 相关
    JobId(String),
    JobDetail(JobDetail),
    
    // CPU 相关
    CpuUsage(f64),  // CPU 使用百分比
    CpuSample(CpuSample),  // CPU 使用样本（带时间戳）
    
    // 内存相关
    MemoryUsage(MemoryUsage),
    
    // 节点信息
    NodeInfo(NodeInfo),
    
    // 统计信息
    Count(u64),
    
    // 错误
    Error(String),
}

#[derive(Debug, Clone)]
pub struct JobDetail {
    pub job_id: String,
    pub status: JobStatus,
    pub start_time: String,
    pub duration: String,
    pub resources_used: ResourceUsage,
}

#[derive(Debug, Clone)]
pub struct ResourceUsage {
    pub cpu_time_ms: u64,
    pub memory_bytes: u64,
    pub io_bytes: u64,
}

#[derive(Debug, Clone)]
pub enum JobStatus {
    Running,
    Ready,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct CpuSample {
    pub timestamp: String,
    pub usage_percent: f64,
    pub load_average: f64,
}

#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub usage_percent: f64,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub host: String,
    pub port: u16,
    pub cpu_count: u32,
    pub memory_total: u64,
    pub memory_available: u64,
    pub uptime: String,
    pub version: String,
    pub active_connections: u32,
}
```

## 网络接口定义

```rust
use futures::Stream;
use std::pin::Pin;

// Network trait 定义
#[async_trait::async_trait]
pub trait DistNetwork {
    // ... 现有方法 ...
    
    // 统一流式接口
    async fn node_operate_stream(
        &self, 
        node_id: NodeId, 
        operation: NodeOperation
    ) -> DistResult<Pin<Box<dyn Stream<Item = DistResult<NodeOperateStreamItem>> + Send>>>;
    
    async fn cluster_operate_stream(
        &self, 
        operation: NodeOperation
    ) -> DistResult<Pin<Box<dyn Stream<Item = DistResult<NodeOperateStreamItem>> + Send>>>;
}
```

## 使用示例

```rust
// 获取集群中所有运行的作业
let operation = NodeOperation::GetRunningJobs {
    limit: None,
    offset: None,
};

let mut stream = runtime.network.cluster_operate_stream(operation).await?;
while let Some(item) = stream.next().await {
    match item?.result {
        StreamResult::JobId(job_id) => println!("Running job: {}", job_id),
        StreamResult::Error(err) => eprintln!("Error: {}", err),
        _ => {} // 忽略其他类型的响应
    }
}

// 获取单个节点的CPU使用情况
let operation = NodeOperation::GetCpuUsage {
    sample_interval_ms: Some(1000),
    sample_count: Some(5),
};

let mut stream = runtime.network.node_operate_stream(target_node, operation).await?;
while let Some(item) = stream.next().await {
    match item?.result {
        StreamResult::CpuUsage(usage) => println!("CPU Usage: {:.2}%", usage),
        StreamResult::CpuSample(sample) => println!("CPU Sample at {}: {:.2}%", sample.timestamp, sample.usage_percent),
        _ => {}
    }
}
```

## 扩展说明

如果需要添加新的操作类型，可以：

1. 在 `NodeOperation` 枚举中添加新的变体并定义所需参数
2. 在 `StreamResult` 中添加相应的响应类型（如果需要）
3. 在服务端实现中处理新的操作类型

这种设计确保了类型安全和代码的可维护性。
