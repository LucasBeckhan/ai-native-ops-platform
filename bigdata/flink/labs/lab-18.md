# Lab 18：Event Time、Watermark 与 Checkpoint 恢复

> **前置**：Flink 集群健康（2 TM × 2 Slot）；jar 已构建（`./scripts/build-flink.sh`）
> **预计耗时**：60 分钟
> **实验目标**：① 观察乱序数据的事件时间窗口；② 亲手触发故障并观察 Checkpoint 恢复。

## 实验一：乱序数据的事件时间窗口（20 分钟）

```bash
./scripts/build-flink.sh    # 首次构建（依赖已缓存则秒级）
docker exec flink-jobmanager /opt/flink/bin/flink run \
  -c com.teleops.flink.m18.EventTimeWindowJob /opt/flink/usrlib/teleops-flink-jobs.jar
```

**预期输出**（TaskManager 日志，`docker logs ai-native-ops-flink-taskmanager-N | grep 1s窗口聚合`）：

```
1s窗口聚合> 设备=eNB-1001 窗口=[1793294646000,1793294647000) avg_throughput=821.4444444444445
1s窗口聚合> 设备=eNB-1001 窗口=[1793294647000,1793294648000) avg_throughput=824.3333333333334
...
```

**观察任务**：
- [ ] 窗口边界是**整秒对齐**且由事件时间决定（数据源乱序 0~300ms，但窗口归属不变）；
- [ ] 平均吞吐稳定在 ~800-825（数据生成区间 800-850，说明乱序事件被正确归窗）；
- [ ] Web UI（localhost:8081）查看该作业的 JobGraph：Source→Window→Sink 已链合并。

## 实验二：故障注入与 Checkpoint 恢复（25 分钟）

```bash
docker exec flink-jobmanager /opt/flink/bin/flink run \
  -c com.teleops.flink.m18.StateCheckpointJob /opt/flink/usrlib/teleops-flink-jobs.jar
```

> ⚠️ 参数顺序注意：`-c 类名` 必须在 jar 路径**之前**（本项目实测踩过：放在 jar 后会被当成程序参数，实际又跑了上一个作业——这也是"作业输出不符合预期"的经典排查点）。

**预期输出时间线**（`docker logs` 带 --timestamps 观察）：

```
17:26:50.120  KeyedState计数> 设备 eNB-1001 累计处理 20 条 KPI      ← 第一次运行
17:26:50.327  java.lang.RuntimeException: 【注入故障】处理第 50 条时进程崩溃
              （JobMaster 日志可见 RestartBackoff: 3s 后重启）
17:26:54.261  KeyedState计数> 设备 eNB-1001 累计处理 20 条 KPI      ← 恢复后继续（状态从 checkpoint 恢复）
17:26:55.266  KeyedState计数> 设备 eNB-1003 累计处理 20 条 KPI
```

**复盘四问**：
1. 故障前后都打印"累计 20 条"——恢复后的计数是从 0 重来还是接着走？（接着走：ValueState 从 Checkpoint 恢复）
2. 恢复后输入是否重放？如何证明？（重放：SourceFunction 无位点状态，从头发；两次"20 条"对应的输入区间有重叠——这正是 at-least-once）
3. 作业日志里能找到几次 Checkpoint 完成？（观察 `Completed checkpoint N for job`）
4. 要做到"输入也不重"，Source 需要什么能力？（位点入状态：Kafka Source 的 offset 就是这么做的）

## 实验三：Checkpoint 与 Slot 观察（15 分钟）

```bash
curl -s localhost:8081/taskmanagers | python3 -m json.tool | grep -E '"path"|"slotsNumber"|"freeSlots"'
curl -s localhost:8081/jobs/overview | python3 -m json.tool
```

**预期**：2 个 TaskManager 各 2 Slot；jobs/overview 中已完成的作业 state=FINISHED。
**思考**：并行提交两个流作业（各并行度 2）会占满 4 个 Slot，第三个作业会 `NoResourceAvailableException`——本项目 lab-20 实测踩过，这就是"Slot 规划 = 并行度预算"的直观理解。
