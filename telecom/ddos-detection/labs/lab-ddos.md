# Lab：DDoS 检测与自动防御端到端

> **前置**：Kafka/Flink 健康；Flink jar 已构建；Spark 侧按"运行方式"准备
> **预计耗时**：100 分钟
> **实验目标**：① CEP 攻击检测；② 三级权限下的自动防御；③ Spark 离线攻击画像。

## 实验一：SYN Flood 检测（30 分钟，复用 lab-19 实验二）

```bash
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.teleops.flink.m19.CepIntrusionJob /opt/flink/usrlib/teleops-flink-jobs.jar

python3 telecom/ddos-detection/generator/netflow_generator.py --scenario synflood
# 运行 60s+ 后注入 ts=+65s 推进事件关闭 CEP 窗口（方法见 lab-19 实验二）
```

**预期告警（实测）**：

```
DDoS告警> {"type":"ddos_alert","attack_type":"SYN_Flood","src_ip":"203.0.113.88",
           "syn_count":6,"severity":"critical"}
```

## 实验二：三级权限的自动防御（30 分钟，★ 本 lab 核心）

```bash
# admin 角色：封禁规则自动下发
python3 telecom/ddos-detection/responder/ddos_responder.py \
  --attack-ip 203.0.113.88 --role admin
```

**预期（实测）**：

```
执行回执: {"ok": true, "already_blocked": false,
  "rule": {"ip": "203.0.113.88", "status": "active", "action": "drop",
           "expires_at": ...}, 
  "confirm": "防火墙已下发 drop 规则: 203.0.113.88（30 分钟后自动解除）"}
```

```bash
# engineer 角色：写操作被挂起审批
python3 telecom/ddos-detection/responder/ddos_responder.py \
  --attack-ip 203.0.113.99 --role engineer
# 预期: [需人工审批] role=engineer tool=firewall-mcp-server.add_firewall_rule level=write

# 危险操作：黑洞路由被硬拒（任何角色）
# 预期: emergency_blackhole 属于 dangerous 级操作...
```

**验证封禁持久化与幂等**：

```bash
cat /tmp/firewall_rules.json | python3 -m json.tool   # 规则带 expires_at
# 重复执行 admin 封禁同一 IP → already_blocked: true（幂等）
```

**复盘三问**：
1. 为什么 write 级用审批而 dangerous 直接拒绝？（文档 1 的权限矩阵设计理由）
2. 如果告警风暴触发 100 次 responder 会发生什么？（幂等保护，只封一次）
3. 封禁 30 分钟自动解除的用意？（防"封了忘解"二次事故）

## 实验三：Spark 离线攻击画像（40 分钟）

```bash
# 1. 准备连接器 jar
./scripts/download-spark-jars.sh

# 2. 宿主机 local 模式运行（环境准备见场景二文档 2.1）
export JAVA_HOME=<JDK8+>
export SPARK_HOME=$(python3 -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")
export PATH="$SPARK_HOME/bin:$PATH"
KAFKA_BOOTSTRAP=localhost:39092 spark-submit --master "local[4]" \
  --jars bigdata/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,bigdata/spark/jars/kafka-clients-3.4.1.jar,bigdata/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,bigdata/spark/jars/commons-pool2-2.12.1.jar \
  telecom/ddos-detection/jobs/offline_attack_analysis.py
```

**预期输出（实测）**：

```
== Top10 可疑源 IP（按纯 SYN 占比） ==
|src_ip       |total_flows|syn_flows|avg_bytes|syn_ratio|
|203.0.113.88 |289        |289      |60.19    |1.0      |   ← 本次攻击
|203.0.113.77 |14         |14       |NULL     |1.0      |   ← lab-19 的历史攻击（回溯！）
|198.51.100.9 |32         |0        |253721.76|0.0      |   ← 正常用户

== 攻击样态 vs 正常样态特征对比 ==
|is_attack_like|avg_bytes|avg_packets|
|true          |60.2     |1.0        |    ← 高包低字节：SYN Flood 指纹
|false         |249920.0 |245.5      |
```

**复盘**：离线层把 lab-19 的历史攻击源也回溯出来了（.77、.66）——"实时挡住 + 离线画像"的双层价值。

## 清理

```bash
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JobID>
rm -f /tmp/firewall_rules.json
```
