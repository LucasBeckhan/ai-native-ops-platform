# =====================================================================
# 文件名: docker/flink-local/Dockerfile
# 所属模块: 基础设施层 —— Flink 1.18.1 本地镜像构建
# 说明:
#   - 为什么本地构建：Docker daemon 代理不可用无法拉取 apache/flink 官方
#     镜像；改为"宿主机 curl 下载 Apache 官方二进制发行包（走可用代理或
#     国内镜像源）+ COPY 进镜像"。与官方镜像同源（同一份 apache-dist
#     发行包），仅少了官方的 entrypoint 包装脚本，这里用等价的启动命令。
#   - 基础镜像 eclipse-temurin:17-jdk（本机已有）。Flink 1.18 支持
#     Java 8/11/17 运行时；选 17 与课程技术栈一致。
#   - 额外内置：Kafka 连接器 jar（flink-sql-connector-kafka 3.0.2-1.18）
#     放入 /opt/flink/lib，使 Kafka Source/Sink 开箱可用。该 jar 由
#     scripts/download-flink-deps.sh 提前下载到构建上下文。
# =====================================================================
FROM eclipse-temurin:17-jdk

WORKDIR /opt

# Apache 官方二进制发行包（构建上下文 = docker/dist）
COPY dist/flink-1.18.1-bin-scala_2.12.tgz /tmp/flink.tgz
# Kafka 连接器（模块 19/20 与场景一/二依赖；下载脚本见 docker/flink-local/download-deps.sh）
COPY dist/flink-sql-connector-kafka-3.0.2-1.18.jar /tmp/

RUN tar -xzf /tmp/flink.tgz -C /opt && \
    mv /opt/flink-1.18.1 /opt/flink && \
    rm /tmp/flink.tgz && \
    mv /tmp/flink-sql-connector-kafka-3.0.2-1.18.jar /opt/flink/lib/ && \
    groupadd --gid 999 flink && \
    useradd --gid 999 --uid 999 -m flink && \
    mkdir -p /opt/flink/usrlib /opt/flink/checkpoints /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink

# 会话集群的默认配置（compose 中可用 FLINK_PROPERTIES 覆盖）
RUN echo "\
jobmanager.rpc.address: flink-jobmanager\n\
jobmanager.memory.process.size: 768m\n\
taskmanager.memory.process.size: 1024m\n\
taskmanager.numberOfTaskSlots: 2\n\
rest.port: 8081\n\
rest.flamegraph.enabled: true\n\
state.checkpoints.dir: file:///opt/flink/checkpoints\n\
state.savepoints.dir: file:///opt/flink/savepoints\n\
" > /opt/flink/conf/flink-conf.yaml && \
    chown flink:flink /opt/flink/conf/flink-conf.yaml

USER flink
WORKDIR /opt/flink

EXPOSE 8081 6123 6124

# 入口：docker-entrypoint.sh 兼容 jobmanager/taskmanager 两种角色，
# 用 su 降权运行（保持与官方镜像一致的用法：command: jobmanager）
COPY --chown=flink:flink flink-local/docker-entrypoint.sh /opt/flink/bin/teleops-entrypoint.sh
RUN chmod +x /opt/flink/bin/teleops-entrypoint.sh
ENTRYPOINT ["/opt/flink/bin/teleops-entrypoint.sh"]
