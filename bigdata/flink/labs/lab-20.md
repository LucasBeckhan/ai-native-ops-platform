# Lab 20：完整监控管道（Kafka/注入源 → CEP → Kafka + Elasticsearch）

> **前置**：lab-18/19 完成；ES 健康（localhost:9200）；**先释放 Slot**（`flink list` + `cancel`）
> **预计耗时**：60 分钟
> **实验目标**：跑通"检测→双写（Kafka+ES）"完整管道，并验证 ES 侧可检索。

## 实验一：管道运行与 ES 验证（30 分钟）

```bash
# 0. Slot 检查（本项目实测：Slot 被占会 NoResourceAvailableException）
curl -s localhost:8081/taskmanagers | python3 -c "
import json,sys
for tm in json.load(sys.stdin)['taskmanagers']:
    print(tm['path'].split('=')[-1][:20], 'freeSlots:', tm['freeSlots'])"

# 1. 提交（默认注入源：每 7 秒产生一组 3 连击链路抖动）
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.teleops.flink.m20.EsAlertPipelineJob /opt/flink/usrlib/teleops-flink-jobs.jar

# 2. 等待 CEP 窗口关闭（60s within + 数据节奏），然后验证
sleep 75
curl -s localhost:9200/telecom-alerts/_count
```

**预期输出（实测）**：

```
{"count":113,"_shards":{...}}
```

```bash
# 3. 检索样例告警
curl -s "localhost:9200/telecom-alerts/_search?size=1" | python3 -m json.tool | head -14
```

**预期样例（实测）**：

```json
{
  "@timestamp": 1793296269224,
  "type": "link_flap",
  "device_id": "eNB-1013",
  "flap_count": 3,
  "severity": "major",
  "message": "链路 3 次闪断"
}
```

## 实验二：Flink 指标接入 Prometheus（20 分钟）

```bash
# 1. 启用 Prometheus Reporter（官方发行包自带 jar，复制到 plugins 即生效）
docker exec flink-jobmanager bash -c \
  "mkdir -p /opt/flink/plugins/prometheus && cp /opt/flink/opt/flink-metrics-prometheus*.jar /opt/flink/plugins/prometheus/"
docker exec flink-jobmanager bash -c \
  "echo 'metrics.reporter.prom.factory.className: org.apache.flink.metrics.prometheus.PrometheusReporterFactoryClass' >> /opt/flink/conf/flink-conf.yaml && echo 'metrics.reporter.prom.port: 9250' >> /opt/flink/conf/flink-conf.yaml"
# 注：类名以镜像内 jar 实际为准（org.apache.flink.metrics.prometheus.PrometheusReporterFactory）

# 2. 重启集群生效（作业需重新提交）
cd docker && docker compose -f docker-compose.flink.yml restart && cd ..
```

**预期**：`curl localhost:9250/metrics`（JobManager 容器内）返回 `flink_job_...` 指标；
`docker/monitoring/prometheus/prometheus.yml` 的 flink job（flink-jobmanager:9250）抓取转绿。

## 实验三：反压与火焰图观察（10 分钟，可选）

1. Web UI localhost:8081 → 运行中作业 → Backpressure 标签：观察各算子 busy/idle 占比；
2. 作业详情 → 开启 Flamegraph（compose 已配 `rest.flamegraph.enabled: true`）；
3. 本管道数据量小（无反压）——观察"全 idle"也是实验结论：反压定位是**有流量压力**时的方法，lab-20 文档第 1 节的表格用于真实瓶颈场景对照。

## 清理

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JobID>
# 删除 ES 实验索引（可选）
curl -X DELETE localhost:9200/telecom-alerts
```
