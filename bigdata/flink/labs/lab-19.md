# Lab 19：Kafka→Flink 实时告警 + CEP 攻击检测

> **前置**：lab-18 完成；Kafka 集群健康
> **预计耗时**：75 分钟
> **实验目标**：① 跑通 Kafka→Flink→Kafka 告警链路；② 亲手复现"开放量词 + watermark"的 CEP 输出语义（本 lab 最大价值点）。

## 实验一：KPI 告警链路（25 分钟）

```bash
# 1. 提交（detached 模式，流作业不会自己结束）
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.teleops.flink.m19.KafkaKpiAlertJob /opt/flink/usrlib/teleops-flink-jobs.jar

# 2. 注入低吞吐 KPI（触发阈值 700Mbps 告警）
python3 - <<'EOF'
from confluent_kafka import Producer
import json, time
p = Producer({"bootstrap.servers": "localhost:39092,localhost:39093,localhost:39094"})
now = int(time.time() * 1000)
for i in range(40):   # 40 条 × 1s 间隔的事件时间，覆盖一个 30s 窗口以上
    p.produce("telecom-gnb-kpi", key=b"eNB-1001",
              value=json.dumps({"deviceId": "eNB-1001", "throughput": 600.0,
                                "ts": now + i * 1000}).encode())
    p.poll(0); time.sleep(0.05)
p.flush(10); print("已注入 40 条 600Mbps 低吞吐 KPI")
EOF

# 3. 等待窗口（30s）+ watermark（5s 乱序度）推进后观察
sleep 45
docker logs ai-native-ops-flink-taskmanager-1 2>&1 | grep "KPI告警" | tail -2
```

**预期输出**：

```
KPI告警> {"type":"kpi_alert","device_id":"eNB-1001","window_start":...,
          "avg_throughput":600.0,"threshold":700.0,"severity":"critical"}
```

验证告警同时写入了 Kafka（Agent 的输入侧）：

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic telecom-alerts \
  --from-beginning --max-messages 2 --timeout-ms 8000 2>/dev/null | head -2
```

## 实验二：CEP 的 watermark 语义（30 分钟，★ 本 lab 核心）

```bash
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.teleops.flink.m19.CepIntrusionJob /opt/flink/usrlib/teleops-flink-jobs.jar
```

```bash
# 注入 SYN 洪水：同一攻击者 6 次 SYN（60s 内 ≥5 次满足条件）
python3 - <<'EOF'
from confluent_kafka import Producer
import json, time
p = Producer({"bootstrap.servers": "localhost:39092,localhost:39093,localhost:39094"})
now = int(time.time() * 1000)
for i in range(6):
    p.produce("telecom-netflow", key=b"203.0.113.88",
              value=json.dumps({"src_ip": "203.0.113.88", "flags": "S",
                                "dst_port": 80, "ts": now}).encode())
p.poll(0)
# 注入 ts=+65s 的"未来"正常流量：把 watermark 推过 within 窗口末端
for i in range(3):
    p.produce("telecom-netflow", key=b"198.51.100.9",
              value=json.dumps({"src_ip": "198.51.100.9", "flags": "SA",
                                "dst_port": 443, "ts": now + 65_000}).encode())
p.flush(10); print("注入完成（SYN×6 + 推进×3）")
EOF

sleep 8
docker logs ai-native-ops-flink-taskmanager-1 2>&1 | grep "DDoS告警" | tail -2
```

**预期输出（实测）**：

```
DDoS告警:2> {"type":"ddos_alert","attack_type":"SYN_Flood","src_ip":"203.0.113.88",
             "syn_count":6,"first_seen":1793296019045,"severity":"critical"}
```

**三个复盘问题（对应文档 3.1 的两个语义 + 一个生产坑）**：
1. 为什么 SYN 注入完**不会立刻**有输出？——`timesOrMore` 是开放量词，要等 watermark 关闭 60s 窗口（所以需要 ts=+65s 的推进事件）；
2. 输出里 syn_count 有 6 也有 5（两次匹配）——开放量词的"部分匹配"也会输出，业务去重交给下游（或改 `times(5)` 精确量词）；
3. 如果去掉 `withIdleness` 会怎样？——本 lab 的第一版作业就是这么写的：6 个空闲 Kafka 分区把全局 watermark 冻结在初始值，**永远不输出**（本项目实测）。生产 Kafka 作业必配。

## 清理

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list   # 记下 RUNNING 的 JobID
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JobID>   # 释放 Slot 给后续实验
```
