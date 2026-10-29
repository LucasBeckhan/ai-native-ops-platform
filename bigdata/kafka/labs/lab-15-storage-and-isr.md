# Lab 15：Kafka 存储机制与 ISR 观察

> **前置**：已完成《模块 15》文档阅读；Kafka 集群已启动
> **预计耗时**：60 分钟
> **实验目标**：① 亲眼验证 .log/.index/.timeindex 三件套与 Segment 滚动；② 用数据理解顺序写优势；③ 掌握 describe 命令观察 ISR/LEO/HW；④ 亲手触发一次副本失效与 ISR 收缩。

**环境自检**（三节点都应为 Up/healthy）：

```bash
cd docker && docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.kafka.yml ps
# 预期输出（Status 列）：
# kafka-1    Up 2 minutes (healthy)
# kafka-2    Up 2 minutes (healthy)
# kafka-3    Up 2 minutes (healthy)
# kafka-ui   Up 2 minutes
```

## 实验一：创建 Topic 并观察磁盘三件套（15 分钟）

```bash
cd bigdata/kafka/scripts && ./create-topics.sh telecom-gnb-kpi
```

**预期输出**（节选）：

```
== 创建 Topic: telecom-gnb-kpi (partitions=6, RF=3, min.isr=2)
Created topic telecom-gnb-kpi.
== 全部 Topic 列表:
telecom-firewall-events
telecom-gnb-alarms
telecom-gnb-kpi
telecom-netflow
telecom-alerts
```

灌入少量数据并探查磁盘布局：

```bash
# 用 kafka-console-producer 灌 100 条消息
for i in $(seq 1 100); do echo "gnb-kpi-sample-$i"; done | \
  docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic telecom-gnb-kpi

python3 scripts/explore_storage.py --ls
```

**预期输出**（节选，每个分区一个目录、内部成组出现三种文件）：

```
== 数据目录: /tmp/kafka-logs
分区目录: telecom-gnb-kpi-0
1.0K /tmp/kafka-logs/telecom-gnb-kpi-0/00000000000000000000.index
 15K /tmp/kafka-logs/telecom-gnb-kpi-0/00000000000000000000.log
1.0K /tmp/kafka-logs/telecom-gnb-kpi-0/00000000000000000000.timeindex
分区目录: telecom-gnb-kpi-1
...
```

验证点：
- [ ] `.log` 有体积而 `.index/.timeindex` 只有 1KB 左右 → **稀疏索引**（每 4KB 数据才写一条索引项）；
- [ ] 6 个分区在 kafka-1 上只出现 3 个左右（其余副本在别的 Broker）→ **RF=3 均匀分布**；
- [ ] `__cluster_metadata-0`（KRaft 元数据日志）也在本目录 → **元数据与数据共用 Segment 格式**。

```bash
python3 scripts/explore_storage.py --dump telecom-gnb-kpi 0
```

**观察任务**：在 `--dump` 的十六进制输出里找到 `baseOffset`（头 8 字节大端序）为全 0 的 batch 头部。不需要逐字节读懂协议，确认"消息是二进制批 + 连续 offset"即可。

## 实验二：顺序写 vs 随机写基准（10 分钟）

```bash
python3 scripts/seq_write_benchmark.py
```

**预期输出量级**（Apple M 系列 NVMe + 每兆节 fsync 实测示例，绝对值因机器而异）：

```
A 顺序追加写       :   0.24s  吞吐    533.3 MB/s  IOPS    136531
B 随机读改写(B树式):   3.50s  吞吐     36.6 MB/s  IOPS      9365

B 耗时是 A 的 14.58 倍
```

**解读任务**（脚本末尾会打印同样的指引）：
- 若 B 明显慢于 A：印证 Kafka 选择纯顺序日志的动机——传统存储引擎每次更新要"读一页、改一页、写回一页"，随机 I/O 在 HDD/SATA SSD 时代慢 10~100 倍；
- 若两者接近（部分高端 NVMe 会出现）：思考 Kafka 吞吐优势的其他来源（批处理、零拷贝、页缓存）；
- 本实验在 macOS APFS（写时复制文件系统）上数值波动较大，建议在 Linux ext4 环境复测；
- 结合文档 2.3 思考：Kafka 为何刻意不做每条消息 fsync？（答案：靠多副本而非单机刷盘换持久性，页缓存命中率才是读性能关键。）

## 实验三：观察 ISR、LEO 与 HW（15 分钟）

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:39092 \
  --describe --topic telecom-gnb-kpi
```

**预期输出**（Leader 分布应大致均衡）：

```
Topic: telecom-gnb-kpi	TopicId: xxxx	PartitionCount: 6	ReplicationFactor: 3
	Topic: telecom-gnb-kpi	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: telecom-gnb-kpi	Partition: 1	Leader: 2	Replicas: 1,2,3	Isr: 1,2,3
	...
```

再看 LEO 与消费位移（`--describe --groups` 见模块 16/17）。对 HW 的直接观察需要启用 `kafka-log-dirs`/日志埋点，本实验用**间接证明**：现在所有 Follower 追平，HW≈LEO，因此新增消费者能立刻读到全部 100 条。

## 实验四：亲手触发 ISR 收缩与恢复（20 分钟）

```bash
# 1. 暂停 kafka-3 的网络能力——用 SIGSTOP 冻结进程模拟"活但跟不上"
docker pause kafka-3

# 2. 持续生产 40 秒（超过 replica.lag.time.max.ms=30s）
for i in $(seq 1 400); do echo "isr-test-$i"; sleep 0.1; done | \
  docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic telecom-gnb-kpi &

# 3. 约 35 秒后观察 ISR（应看到部分分区 Isr 中 3 消失）
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:39092 \
  --describe --topic telecom-gnb-kpi | head -8
```

**预期输出**（`Isr` 列不再包含 3）：

```
Topic: telecom-gnb-kpi	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2
```

恢复并确认 ISR 扩展回来：

```bash
docker unpause kafka-3
sleep 10
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:39092 \
  --describe --topic telecom-gnb-kpi | head -8
# 预期：Isr 恢复为 1,2,3（追平 LEO 后自动扩展回 ISR）
```

**复盘三问**：
1. 暂停期间生产一直成功（acks=all 且 ISR≥2 满足），如果同时把 kafka-2 也暂停会发生什么？（答：ISR=1 < min.isr=2，生产端抛 `NotEnoughReplicasException`——**宁可不可写也不丢数**。）
2. 若此时 kafka-1（某分区 Leader）彻底宕机，新 Leader 会从谁里选？（答：仅 ISR 内副本；kafka-3 不在 ISR，即使它数据"看起来新"也无资格——`unclean.leader.election.enable=false`。）
3. 这个实验模拟的是哪类真实生产故障？（答：Follower GC 停顿/磁盘慢/网络抖动导致的 ISR 抖动，模块 17 会展开监控指标 UnderReplicatedPartitions。）

## 清理

```bash
docker compose -f ../../docker/docker-compose.kafka.yml down   # 数据卷保留
# 彻底重来: down -v
```
