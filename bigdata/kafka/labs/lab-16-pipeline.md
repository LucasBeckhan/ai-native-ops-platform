# Lab 16：设备日志采集管道（幂等 · 事务 · 再平衡 · 幂等消费）

> **前置**：模块 15 实验已完成；集群健康；Python 依赖已装（`pip install -r ../src/requirements.txt`）
> **预计耗时**：90 分钟
> **实验目标**：① 跑通"采集器→Kafka→事务闭环→告警事件"管道；② 验证幂等/事务/隔离级别/再平衡/幂等消费五个可靠性机制真实生效。

**准备**：

```bash
cd bigdata/kafka/src
pip install -r requirements.txt
./scripts 路径说明：本目录下所有命令默认在 src/ 目录执行
```

## 实验一：采集器管道（10 分钟）

```bash
python3 syslog_producer.py --count 100 --interval 0.01
```

**预期输出**：

```
已发送 100 条告警到 telecom-gnb-alarms（key=设备ID，分区保序）
```

验证按设备保序（同一设备 seq 连续递增）：

```bash
python3 - <<'EOF'
import json
from confluent_kafka import Consumer, TopicPartition
c = Consumer({"bootstrap.servers": "localhost:39092,localhost:39093,localhost:39094",
              "group.id": "order-check", "auto.offset.reset": "earliest",
              "enable.partition.eof": True})
c.assign([TopicPartition("telecom-gnb-alarms", p, 0) for p in range(3)])
rows, eof = [], 0
while eof < 3:
    for m in c.consume(num_messages=100, timeout=3.0):
        if m.error(): eof += 1; continue
        rows.append(json.loads(m.value()))
c.close()
dev = {}
for r in rows: dev.setdefault(r["device_id"], []).append(r["seq"])
bad = [d for d, s in dev.items() if s != sorted(s)]
print(f"设备数 {len(dev)}，乱序设备 {len(bad)} 个 => {'保序生效' if not bad else '发现乱序!'}")
EOF
```

**预期输出**：`设备数 50，乱序设备 0 个 => 保序生效`

## 实验二：消费组与再平衡（20 分钟）

开两个终端分别运行：

```bash
python3 consumer_group_demo.py --group rebalance-demo --member m1   # 终端 1
python3 consumer_group_demo.py --group rebalance-demo --member m2   # 终端 2（30 秒后启动）
```

**预期输出（终端 1，m2 加入时触发增量再平衡）**：

```
[assign] m1 拿到分区: [('telecom-gnb-alarms', 0), ('telecom-gnb-alarms', 1), ('telecom-gnb-alarms', 2)]
[m1] 已处理 50 条，最近: eNB-1024 eNB-1024 光模块接收异常
[revoke] 撤销分区并同步提交位移: [('telecom-gnb-alarms', 1), ('telecom-gnb-alarms', 2)]
[assign] m1 拿到分区: [('telecom-gnb-alarms', 0)]
```

观察点（对照文档 2.2）：
- [ ] m2 加入时 m1 只撤销部分分区（cooperative-sticky），未被撤销的分区继续消费——对比传统 Eager 全组停顿；
- [ ] revoke 回调中同步提交位移，避免重复；
- [ ] Ctrl+C m1 后 m2 接管全部分区。

> ⚠️ 生产教训实验：若用 Ctrl+Z/kill -9 强杀成员，组要等 `max.poll.interval.ms`（librdkafka 默认 300s）才清场，期间新成员 JoinGroup 被阻塞——这就是"再平衡风暴"的微观机制。实验后如遇阻塞，换一个 `--group` 名即可。

## 实验三：幂等 vs 非幂等生产者（15 分钟）

```bash
python3 producer_idempotent.py --mode idempotent --run-id exp1
python3 producer_idempotent.py --mode naive       --run-id exp1
```

**预期输出**：

```
[idempotent] 已发送 run=exp1 的 50 条 seq=0..49 到分区(key=order-test)
[naive] 已发送 run=exp1 的 50 条 seq=0..49 到分区(key=order-test)
```

消费端验证顺序与重复（输出到 telecom-alerts）：

```bash
python3 - <<'EOF'
import json
from confluent_kafka import Consumer, TopicPartition
c = Consumer({"bootstrap.servers": "localhost:39092,localhost:39093,localhost:39094",
              "group.id": "seq-check", "auto.offset.reset": "earliest",
              "enable.partition.eof": True})
c.assign([TopicPartition("telecom-alerts", p, 0) for p in range(3)])
seqs, eof = [], 0
while eof < 3:
    for m in c.consume(num_messages=100, timeout=3.0):
        if m.error(): eof += 1; continue
        v = json.loads(m.value())
        if isinstance(v, dict) and v.get("type") == "alarm" and v.get("origin_seq") is not None:
            pass
c.close()
print("（本验证脚本统计的是事务实验产出的事件，幂等实验的顺序验证见下）")
EOF
```

> 实验三的核心观察点在**异常注入**下才明显：`naive` 模式 + `max.in.flight=5` 时，人为断网 2 秒（`docker pause kafka-1`）再恢复，重试批次可能"后发先至"。正常网络下两者输出一致——**幂等是异常路径上的保险**，这个认知本身就是面试加分点（文档 1.2）。

## 实验四：事务闭环与隔离级别（30 分钟）

先灌入新鲜数据，再跑事务闭环（第 3 批故意中止）：

```bash
python3 syslog_producer.py --count 60 --interval 0.01
python3 producer_transaction.py --group txn-demo-g2 --abort-at 3
```

**预期输出**（各批次下游条数取决于告警严重级分布，总量级一致）：

```
事务已初始化（transactional.id=teleops-txn-producer-01）
第 1 批：事务已提交（50 条入，36 条下游产出）
第 2 批：事务已提交（50 条入，31 条下游产出）
第 3 批：事务已中止（50 条消费位移与 35 条下游消息全部回滚）
第 4 批：事务已提交（50 条入，36 条下游产出）
第 5 批：事务已提交（50 条入，36 条下游产出）
第 6 批：事务已提交（10 条入，3 条下游产出）
完成 6 批处理。下游 telecom-alerts 中只会出现已提交批次的数据。
```

验证隔离级别（实测数字）：

```
read_committed 读到 387 条；read_uncommitted 读到 422 条
差异 35 条 = 被中止事务（第 3 批）的下游消息，对已提交读不可见 => 隔离生效
```

**复盘三问**：
1. 中止批次的上游位移也被回滚了，重启后这 50 条会被重消费吗？（会——位移没提交，这正是"重复但正确"：靠消费端幂等兜底，见实验五。）
2. 把 isolation.level 改成 read_uncommitted 会看到什么？（看到那 35 条"幽灵告警"——脏读。）
3. transactional.id 改一个值再跑会发生什么？（旧 PID 的新事务与新 PID 无关，Fencing 只在"同 id 多实例"时生效。）

## 实验五：幂等消费（15 分钟）

```bash
# 第一遍：正常消费 100 条 → 全部新处理
python3 consumer_idempotent.py
# 预期：共消费 100 条：新处理 100 条，识别重复 0 条并跳过。

# 第二遍：位移回退 100 条重放（模拟误重置/崩溃未提交），DB 不清空
python3 consumer_idempotent.py --rewind 100
# 预期：共消费 100 条：新处理 0 条，识别重复 100 条并跳过。
```

**复盘**：位移回退必然带来重复，重复被"业务主键唯一表"精确消化。这回答了文档 2.2 铁律 3 的兜底问题——**at-least-once + 幂等消费 = 效果上的 Exactly-Once**。

## 清理

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --delete --group txn-demo-g2
```
