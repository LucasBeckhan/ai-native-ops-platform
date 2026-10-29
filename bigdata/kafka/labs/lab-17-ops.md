# Lab 17：Kafka 集群运维演练（故障注入 · Lag 排查 · 分区重分配 · 指标开启）

> **前置**：模块 15/16 实验完成，集群三节点健康，Topic 已创建
> **预计耗时**：120 分钟
> **实验目标**：① 亲手宕掉一个 Broker 并完整记录恢复过程；② 掌握 Lag 三种形态的判读；③ 走完分区重分配全流程；④ 打开 JMX 指标接入 Prometheus。

## 实验一：Broker 故障演练（40 分钟）

**基线记录**（演练前必做，这是"演练记录表"的第一列）：

```bash
date +%H:%M:%S                                    # T0 注入时刻
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic telecom-gnb-kpi | head -4
```

**预期输出**（记下每个分区的 Leader）：

```
Topic: telecom-gnb-kpi	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
Topic: telecom-gnb-kpi	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
Topic: telecom-gnb-kpi	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
```

**注入故障**（直接杀掉承载最多 Leader 的节点，这里假设 kafka-1）：

```bash
docker stop kafka-1 && date +%H:%M:%S   # T1
```

立即观察（连续执行 3 次，间隔 5 秒）：

```bash
docker exec kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic telecom-gnb-kpi | head -4
```

**预期输出**（Leader 全部迁到存活节点，Isr 中 1 被移除）：

```
Topic: telecom-gnb-kpi	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,2
Topic: telecom-gnb-kpi	Partition: 1	Leader: 2	Replicas: 1,2,3	Isr: 2,3
Topic: telecom-gnb-kpi	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,3
```

验证写入不中断（min.isr=2 满足，应当成功）：

```bash
echo "post-failure-$(date +%s)" | docker exec -i kafka-2 \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic telecom-gnb-kpi && echo "写入成功（未丢可用性）"
```

**恢复**并记录追平耗时：

```bash
docker start kafka-1 && date +%H:%M:%S   # T2
# 每 10 秒观察一次 ISR，直到 1 回到所有 Isr 列表：
watch -n 10 "docker exec kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic telecom-gnb-kpi | grep -c 'Isr: 1'"
# 输出 6（=分区总数）即全部追平，记 T3
```

**填写演练记录表**（这就是生产 SRE 的标准动作）：

| 注入方式 | T1→可用性恢复 | T2→ISR 追平 | 业务影响 | 暴露盲区 |
|---|---|---|---|---|
| docker stop kafka-1 | ____s | ____s | 写入中断 ____s/未中断 | 例：无 URP 告警面板 |

**进阶实验**：再停掉 kafka-2（此时只剩 1 个节点），验证 `min.insync.replicas=2` 的保护——写入应报 `NOT_ENOUGH_REPLICAS`：

```
预期报错关键行：org.apache.kafka.common.errors.NotEnoughReplicasException
结论：宁可不可写，也不丢数（CP 取向的实证，模块 15 文档 4.2）
```

## 实验二：Consumer Lag 三种形态判读（25 分钟）

制造"个别分区独高"形态（热点 key 倾斜）：

```bash
# 灌入大量同一 key 的消息（模拟热点基站告警风暴）
python3 - <<'EOF'
from confluent_kafka import Producer
import json, time
p = Producer({"bootstrap.servers": "localhost:39092,localhost:39093,localhost:39094"})
for i in range(20000):
    # 全部消息同一个 key → 全部进同一分区 → 制造倾斜
    p.produce("telecom-gnb-kpi", key=b"eNB-9999",
              value=json.dumps({"seq": i, "kpi": "throughput", "v": i}).encode())
    p.poll(0)
p.flush()
print("20000 条热点消息已写入")
EOF

# 起一个"慢消费者"（每条处理 5ms，模拟下游 DB 慢）
python3 - <<'EOF' &
import json, time
from confluent_kafka import Consumer
c = Consumer({"bootstrap.servers": "localhost:39092,localhost:39093,localhost:39094",
              "group.id": "slow-group", "auto.offset.reset": "earliest"})
c.subscribe(["telecom-gnb-kpi"])
while True:
    for m in c.consume(num_messages=10, timeout=1.0):
        if not m.error():
            time.sleep(0.005)   # 慢处理
EOF
sleep 30

# 观察 Lag 形态
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group slow-group
```

**预期输出形态**（个别分区 Lag 显著高于其他分区——注意 key=eNB-9999 的那个分区）：

```
GROUP       TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
slow-group  telecom-gnb-kpi   0          1204            20000           18796
slow-group  telecom-gnb-kpi   1          3312            3400            88
slow-group  telecom-gnb-kpi   2          2877            2950            73
```

**判读练习**（对照文档第 2 节）：这是形态 ②——个别分区独高 → 热点 key 倾斜。给出两个解法（key 加盐 / 下游二次聚合），并说出"直接给这个消费者加实例有没有用"（没用：单分区并行度上限为 1）。

清理：`kill %1`（停掉慢消费者）并删除组。

## 实验三：分区重分配全流程（30 分钟）

把 telecom-gnb-alarms 的 0 号分区从当前副本组迁到指定节点组合（模拟"新 Broker 扩容"场景）：

```bash
# 1. 生成候选计划（--list 查看现状）
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic telecom-gnb-alarms | head -4

# 2. 编写迁移计划 JSON（把 P0 的副本从 {1,2,3} 调整为 {2,3,1}——
#    实验用循环换位演示流程，生产中是"迁到新加入的 Broker 4/5"）
cat > /tmp/reassign.json <<'EOF'
{
  "version": 1,
  "partitions": [
    {"topic": "telecom-gnb-alarms", "partition": 0,
     "replicas": [2, 3, 1], "log_dirs": ["any", "any", "any"]}
  ]
}
EOF
docker cp /tmp/reassign.json kafka-1:/tmp/reassign.json

# 3. 执行（带限流 50MB/s——生产必加，防打爆集群带宽）
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file /tmp/reassign.json --execute --throttle 52428800
```

**预期输出**：

```
The full reorder plan with a throttle factor ...
Reassignment of partition telecom-gnb-alarms-0 is still in progress
```

```bash
# 4. 轮询验证直到完成（副本迁移是异步的数据拷贝）
docker exec kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file /tmp/reassign.json --verify
```

**预期输出**：

```
Reassignment of partition telecom-gnb-alarms-0 is completed.
```

```bash
# 5. 确认新副本分布生效
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic telecom-gnb-alarms | head -2
# 预期：Partition 0 的 Replicas 变为 2,3,1
```

## 实验四：JMX 指标接入 Prometheus（25 分钟）

apache/kafka 官方镜像自带 JMX Exporter agent jar（/opt/kafka/libs 目录无——需挂载）。实验用轻量方式：给 kafka-1 追加环境变量开启 JMX 端口 + Prometheus JMX Exporter（如果本机没有 agent jar，用 kafka 自带 `kafka-run-class.sh` 的 JMX 远程口 + kafka_exporter 替代，二选一）：

**方案 A（推荐，零额外镜像）：Kafka Exporter 容器**

在 `docker/docker-compose.kafka.yml` 追加（或临时 docker run）：

```bash
docker run -d --name kafka-exporter --network teleops-net -p 39200:9210 \
  danielqsj/kafka-exporter --kafka.server=kafka-1:9092 --kafka.server=kafka-2:9092 --kafka.server=kafka-3:9092
```

验证指标（关注 URP 与 Lag）：

```bash
curl -s localhost:39200/metrics | grep -E "kafka_brokers|kafka_topic_partition_under_replicated|kafka_consumergroup_lag" | head -8
```

**预期输出**：

```
kafka_brokers 3
kafka_topic_partition_under_replicated_partition{partition="0",topic="telecom-gnb-kpi"} 0
kafka_consumergroup_lag{consumergroup="slow-group",topic="telecom-gnb-kpi",partition="0"} 18796
```

然后在 Prometheus 的 `docker/monitoring/prometheus/prometheus.yml` 中把 kafka job 的 targets 改为 `["kafka-exporter:9210"]`，Grafana 导入官方 Dashboard ID 7589/11962 即可看到大盘。

**（若 danielqsj/kafka-exporter 镜像拉取受限，方案 B：直接用 kafka 自带脚本轮询并输出，见 scripts/lag_watch.sh）**

## 清理

```bash
docker rm -f kafka-exporter 2>/dev/null
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --delete --group slow-group
```
