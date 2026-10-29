# 环境准备手册（自助运行前的完整清单）

> 本手册假设你是**拿到项目后从头自己搭环境**。所有镜像/依赖的获取命令都在这里；
> 每个模块实验之前，回来查这张表即可。脚本路径均相对项目根目录。

---

## 1. 基础检查

```bash
docker --version          # 需要 Docker 24+ / Compose v2
df -h /System/Volumes/Data   # 磁盘剩余建议 ≥15GB（本项目镜像+数据约 10GB）
```

## 2. 镜像获取总清单（按用途分组）

> 网络通畅：直接 `docker pull`。
> 网络受限：**加 daocloud 前缀拉取后重打标签**（两步模板见下方"离线模板"）。

### 2.1 基础设施（compose 直接引用，版本见 `docker/.env`）

| 镜像 | 用途 | 离线前缀 |
|---|---|---|
| apache/kafka:4.3.1 | 三节点 KRaft 集群 | docker.m.daocloud.io/ |
| apache/flink:1.18.1 | Flink 集群 | docker.m.daocloud.io/ |
| apache/spark:v3.5.5 | Spark 集群 | docker.m.daocloud.io/ |
| docker.elastic.co/elasticsearch/elasticsearch:8.12.2 | 告警检索 | docker.m.daocloud.io/ |
| prom/prometheus:v3.2.1 · grafana/grafana:11.5.1 | 监控 | docker.m.daocloud.io/ |
| provectuslabs/kafka-ui:v0.7.2 | Kafka 可视化 | docker.m.daocloud.io/ |
| milvusdb/milvus:v2.4.4 + etcd + minio | 向量库（RAG，可选） | docker.m.daocloud.io/ |

**离线模板**（以 Flink 为例，其余同理）：

```bash
docker pull docker.m.daocloud.io/apache/flink:1.18.1
docker tag  docker.m.daocloud.io/apache/flink:1.18.1 apache/flink:1.18.1
```

### 2.2 本项目自建镜像（源码+Dockerfile 已在仓库，`--build` 即可）

| 镜像 | 构建方式 | 所属实验 |
|---|---|---|
| teleops/mock-llm:1.0.0 | `docker compose -f docker/docker-compose.llm.yml up -d --build` | 全部 Agent 实验 |
| teleops/agent-api:1.0.0 | `docker compose -f docker/docker-compose.llm.yml -f docker/docker-compose.agent.yml up -d --build` | 模块 34 服务化 |
| teleops/elasticsearch:8.12.2 | 见 `docker/es/Dockerfile`（需先下载 ES 发行包，见 2.4） | 模块 19/20/30 |
| teleops/flink:1.18.1 | 见 `docker/flink-local/Dockerfile`（需先下载 Flink 发行包+连接器，见 2.4） | 模块 18-20 |

### 2.3 K8s 实验（kind 集群专用）

```bash
# kind 节点镜像
docker pull docker.m.daocloud.io/kindest/node:v1.31.4
kind create cluster --name teleops --image kindest/node:v1.31.4 --wait 90s

# ★ 离线关键步骤：宿主机已有的镜像必须"注入"kind 节点才能用
kind load docker-image nginx:1.27 busybox:1.36 --name teleops
kind load docker-image registry.k8s.io/ingress-nginx/controller:v1.11.3 --name teleops
kind load docker-image registry.k8s.io/metrics-server/metrics-server:v0.7.2 --name teleops
```

ingress-nginx 完整部署清单：`k8s/advanced/02-ingress-lab/ingress-nginx-kind.yaml`
metrics-server 精简部署：见 `k8s/advanced/02-ingress-lab/lab-hpa.md` 实验一
（四个排障层实录——证书目录/非 root 端口/RBAC/失败 APIService 毒化控制面）

### 2.4 大文件依赖（脚本自动下载）

```bash
./scripts/download-spark-jars.sh     # Spark Kafka 连接器 ×4（模块 24/场景二）
./scripts/build-flink.sh             # Flink 作业构建（需先有 JDK+Maven，见脚本注释）
# Flink/ES 发行包（构建本地镜像用，各 ~400MB）：
#   Flink:  https://mirrors.tuna.tsinghua.edu.cn/apache/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
#   ES:     https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.12.2-linux-aarch64.tar.gz
#   下载后放 docker/dist/（构建上下文），命令见 docker/es/Dockerfile 头注释
# PySpark（模块 21-24 宿主机 local 模式）：
#   pip install pyspark==3.5.5 -i https://pypi.tuna.tsinghua.edu.cn/simple
# Python 依赖（Mock LLM/Agent/采集器）：
#   pip install confluent-kafka httpx fastapi uvicorn prometheus-client pydantic
```

### 2.5 Go 工具链（模块 11 / 场景四 Operator）

```bash
GOPROXY=https://goproxy.cn,direct go build ./...   # 依赖自动经国内代理下载
```

## 3. 启动顺序（每个模块的实验前状态检查）

```bash
# ① Kafka + Flink + ES + 监控（大多数实验的地基）
cd docker && docker compose -f docker-compose.kafka.yml -f docker-compose.flink.yml \
  -f docker-compose.elasticsearch.yml -f docker-compose.monitoring.yml up -d

# ② Agent 实验加 Mock LLM（端口 18000）
docker compose -f docker-compose.llm.yml up -d --build

# ③ Spark 实验（可选：集群；宿主机 local 模式不需要）
docker compose -f docker-compose.spark.yml up -d

# ④ 健康检查（全绿再开始）
curl -s localhost:18000/healthz; curl -s localhost:9200 | grep cluster_name
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
curl -s localhost:8081/overview | head -c 60; curl -s localhost:9090/-/healthy
```

## 4. 磁盘与运行注意（踩过坑的）

- **磁盘 ≥15GB 再启动全套**：曾因磁盘 100% 引发 Docker 崩溃与控制面故障（复盘见 `k8s/advanced/02-ingress-lab/lab-hpa.md`）
- 不用的服务随手 `docker compose ... stop`（Flask/Flink 常驻各 ~1GB）
- kind 节点镜像注入（2.3）是离线环境**必做**步骤，否则 Pod 全部 ImagePullBackOff
- Kafka 宿主机端口是 **39092-39094**（容器内 9092）；Mock LLM 宿主机端口是 **18000**（容器内 8000）
- Flink 作业提交：`-c 类名` 必须放在 jar 路径**之前**（顺序错了不报错但跑错作业）
