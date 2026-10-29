# 排障实战题（10 道）

> 每道题按"故障现象 → 排查步骤 → 根因分析 → 解决方案 → 预防措施"五段结构。
> 使用方法：先只读"现象"，自己推演排查路径，再对照答案。
> 标注 ★ 的是本项目实验中真实踩到过的问题。

---

## 1. ★ Pod 反复 CrashLoopBackOff 但日志"无异常"

**现象**：Pod 重启循环，`kubectl logs` 显示应用正常输出后戛然而止，无明显报错。
**排查**：
1. `kubectl describe pod` → 看 Last State（OOMKilled? exit code 137?）；
2. `kubectl logs --previous`（上一次崩溃前的日志——默认 logs 只看当前实例）；
3. `kubectl top pod` 对比 limits.memory；
4. 若 exit code = 1 且无日志 → 探针失败？（describe 看 Liveness failure 事件）。
**根因**：三种常见可能——① OOMKilled：limits.memory 小于实际峰值（Java 应用最常见：堆外内存+元空间未计入）；② liveness 探针误杀（依赖服务超时导致健康检查失败）；③ 应用收到 SIGTERM 后非零退出（graceful shutdown 缺失）。
**解决**：按根因调 limits / 解耦 liveness 依赖 / 补优雅退出。
**预防**：内存预留 = 堆 + 1.5×堆外；liveness 只查自身；变更前压测。
**考察点**：`--previous` 与 exit code 的解码能力（137=OOM/SIGKILL）。

## 2. ★ Kafka Consumer Lag 持续增长

**现象**：某消费组 Lag 从 1 万涨到 500 万，业务数据延迟分钟级。
**排查**（分层，模块 17 文档 2 节的方法论）：
1. `kafka-consumer-groups --describe` 看形态：全分区均匀涨 or 个别分区独高；
2. 均匀涨 → 消费端：处理耗时突增？GC？下游 DB 慢？`max.poll.interval` 逼近（日志看再平衡频率）；
3. 个别独高 → 数据倾斜（热点 key），加消费者实例无效（单分区并行度=1）；
4. CONSUMER-ID 为空的分区 → 成员死亡未触发再平衡。
**根因**：本实验实测为热点 key 倾斜（eNB-9999 单 key 2 万条）；另一常见根因是下游数据库慢查询。
**解决**：倾斜→key 加盐+下游二次聚合；慢消费→扩消费者（≤分区数）+优化处理逻辑；可丢弃类→`--reset-offsets --to-datetime` 跳过并事后重算。
**预防**：Lag 分区级监控+基线告警（模块 17 的 lag_watch.sh 思路）；发布前压测消费吞吐。
**考察点**：Lag 三种形态的判读（均匀/倾斜/无人消费）。

## 3. ★ Flink Checkpoint 持续失败

**现象**：Checkpoint 失败率 100%，作业虽在跑但状态无保护。
**排查**：① Coordinator 日志看失败原因（timeout / task failed / IO）；② Web UI 反压页——Barrier 对齐时间占比高=反压拖慢；③ 状态大小是否突增（倾斜 key）；④ 快照存储 IO（目标盘/HDFS 吞吐）。
**根因**：本实验环境实测为 **Slot 不足**（NoResourceAvailable——资源型失败）；生产最常见是反压导致的对齐超时。
**解决**：止血=unaligned checkpoint（牺牲吞吐保快照）+加大 timeout；治本=治反压/扩 Slot/优化倾斜。
**预防**：Checkpoint 失败率纳入 SLO；大作业默认增量 Checkpoint。
**考察点**：反压与 Checkpoint 的联动理解。

## 4. Spark 作业 OOM 但 Executor 内存"已很大"

**现象**：`spark.executor.memory=16g` 仍 OOM（容器被杀 / OutOfMemoryError）。
**排查**：① 确认 OOM 位置：Driver? Executor? 容器层（overhead）?；② 看 task 数据量——是否倾斜（单 task 读入 10GB）；③ Spark UI 的 Storage 内存占用；④ collect()/toPandas() 是否在 Driver 打爆。
**根因**：多数不是"内存小"而是**数据倾斜**（单 task 分区数据爆炸）或 Driver collect 大表；其次是 overhead 不足（堆外/网络缓冲）。
**解决**：倾斜→加盐/广播（模块 23 选择树）；collect→改写盘；overhead→`spark.executor.memoryOverhead` 调大。
**预防**：AQE 开启（自动拆倾斜）；大结果一律写存储不回 Driver。
**考察点**：OOM 位置的区分（driver/executor/container overhead）。

## 5. ★ Agent 调用 Tool 超时导致整个工作流卡死

**现象**：Agent 响应时间从 2s 涨到 5 分钟，日志显示某工具调用一直 pending。
**排查**：① Router 审计日志定位卡在哪个工具（本项目 tool_router.audit 有 ms 记录）；② 该工具的下游（MCP Server 存活？外部 API？）；③ 是否所有请求都卡（连接池耗尽）还是单请求。
**根因**：MCP Server 进程挂起（无超时保护时 Client 永久等待——本项目 router 的 ThreadPoolExecutor timeout=15s 即为此设计）；或连接池未释放（泄漏）。
**解决**：工具调用强制超时（本项目实测实现）；异常 Observation 回喂 Agent 自主改路；Server 健康检查+自动重连。
**预防**：所有外呼带超时；熔断器（连续失败跳过该 Server）；审计日志监控 P99。
**考察点**：超时与熔断在 Agent 工具链的落点。

## 6. RAG 召回率突然骤降

**现象**：知识库问答从"能答对 80%"跌到 30%，用户反馈"答非所问"。
**排查**（模块 30 Q1 的五步法）：① Embedding 服务变更/超时（对比相似度分布）；② 索引重建/文档数变化；③ 分块器版本漂移；④ 查询改写回归；⑤ 语料污染（错误文档入库）。
**根因**：常见为 embedding 模型悄悄升级（向量空间不兼容）或索引重建半途。
**解决**：回滚 embedding 版本或全量重建索引；期间 BM25 关键词通道兜底。
**预防**：RAGAS 评估集定期回归；embedding 模型版本固化+变更走评审。
**考察点**：把"效果下降"工程化为五层排查。

## 7. K8s 集群 DNS 解析偶发失败

**现象**：Service 域名解析间歇性 timeout（`nslookup` 偶发超时），微服务调用随机失败。
**排查**：① CoreDNS Pod 资源（CPU throttling）；② 节点 conntrack 表满（dmesg）；③ `ndots:5` 导致外部域名最多 5 次无效查询（放大流量）；④ NodeLocal DNSCache 是否部署。
**根因**：高并发下 CoreDNS 被打满或 conntrack UDP 记录冲突（DNS over UDP 五元组复用）。
**解决**：部署 NodeLocal DNSCache（节点本地缓存）+ 调 conntrack 上限 + 应用侧访问外部域名用 FQDN（带尾点，绕过 ndots 展开）。
**预防**：DNS 指标监控（CoreDNS cache_hit_ratio）；压测含 DNS。
**考察点**：ndots 与 UDP conntrack 两个隐藏机制。

## 8. Istio Sidecar 注入后服务响应变慢

**现象**：注入 sidecar 后 P99 从 20ms 涨到 45ms。
**排查**：① 对比注入前后的拓扑（每跳多了 in/out 两个 Envoy）；② Envoy CPU 是否限流（throttling）；③ 重试/超时配置叠加放大；④ mTLS 握手开销（首次连接）。
**根因**：额外两跳代理开销 + 默认重试策略叠加（3 次重试 × 高延迟 = 放大）。
**解决**：调连接池与 keepalive；review VirtualService 重试策略；必要时对延迟敏感服务摘除 mesh 或用 eBPF mesh。
**预防**：注入前后基准测试；mesh 延迟预算纳入容量规划。
**考察点**：sidecar 延迟模型（一跳两次代理）。

## 9. etcd 集群 Leader 频繁切换

**现象**：`etcdctl endpoint status` 显示 Leader 一天内切换多次，apiserver 偶发慢。
**排查**：① Leader 切换伴随日志（election timeout）；② 节点间网络 RTT/丢包（跨机房部署？）；③ 磁盘 fsync 延迟（`etcd disk wal fsync duration` 指标 >10ms 危险）；④ 大对象写入（K8s 大 ConfigMap/CR）导致心跳延迟。
**根因**：常见为磁盘 IO 抖动（fsync 慢）或跨机房网络抖动导致选举超时。
**解决**：etcd 用本地 NVMe 独立盘；同机房部署（Raft 对延迟敏感）；拆大对象；调 election timeout（默认 1s，抖动环境可到 2s）。
**预防**：etcd 专属监控（fsync P99、慢 apply）+ 季度 defrag。
**考察点**：Raft 选举与磁盘/网络的联动。

## 10. ★ MCP Server 连接不稳定导致 Agent 工具调用间歇性失败

**现象**：Agent 工具调用成功率 95%，失败日志为 "连接被对端关闭"（stdio 传输下 Server 子进程偶发退出）。
**排查**：① Server 进程日志（OOM? panic?）；② stdio 管道读写竞争（多线程同时写 stdout？）；③ Client 读超时后是否正确回收子进程；④ 失败时间分布（是否与 GC/系统负载相关）。
**根因**：本项目代码审计发现的典型点——Client 侧读超时后子进程未回收产生僵尸；Server 侧多线程写 stdout 无锁导致帧交错。
**解决**：Client 超时回收进程+重连（本项目 mcp_client.close 的 terminate/kill 双保险）；Server 输出加锁；健康检查（ping 方法）。
**预防**：连接池+自动重连（模块 34 的 Harness 生产化）；失败率监控。
**考察点**：进程生命周期管理与管道并发安全。
