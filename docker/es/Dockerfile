# =====================================================================
# 文件名: docker/es/Dockerfile
# 所属模块: 基础设施层 —— Elasticsearch 8.12.2 本地镜像构建
# 说明:
#   - 为什么本地构建：Docker daemon 配置的代理不可用导致无法直接拉取
#     docker.elastic.co 镜像；改为"宿主机 curl 下载官方 tar 包（走可用
#     代理）+ COPY 进镜像"，与官方镜像等价（同为 Elastic 官方发行包）。
#   - 基础镜像 eclipse-temurin:17-jre-jammy 本机已有；ES 8.12 发行包自带
#     JDK17，因此基础镜像只提供 glibc 与工具链。
#   - 若你的网络可直接拉取官方镜像，可忽略本 Dockerfile，使用 .env 中
#     ES_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:8.12.2 即可。
# =====================================================================
FROM eclipse-temurin:17-jre-jammy

# 安装 tar 包必需的少量工具（jammy 自带 tar/gzip，无需额外包）
WORKDIR /opt

# 官方发行包（构建上下文 = docker/dist，先下载好 tar.gz）
COPY elasticsearch-8.12.2-linux-aarch64.tar.gz /tmp/es.tar.gz

# 1. 解压并规整目录；2. 创建非 root 用户（ES 拒绝 root 运行）
RUN tar -xzf /tmp/es.tar.gz -C /opt && \
    mv /opt/elasticsearch-8.12.2 /opt/elasticsearch && \
    rm /tmp/es.tar.gz && \
    useradd -m -u 1000 esuser && \
    mkdir -p /opt/elasticsearch/data /opt/elasticsearch/logs && \
    chown -R esuser:esuser /opt/elasticsearch

# 学习环境基础配置：单节点、关安全（生产必须开启 TLS+RBAC）
RUN echo "\
cluster.name: teleops-es\n\
node.name: es-1\n\
discovery.type: single-node\n\
network.host: 0.0.0.0\n\
http.port: 9200\n\
xpack.security.enabled: false\n\
path.data: /opt/elasticsearch/data\n\
path.logs: /opt/elasticsearch/logs\n\
" > /opt/elasticsearch/config/elasticsearch.yml && \
    chown esuser:esuser /opt/elasticsearch/config/elasticsearch.yml

USER esuser
WORKDIR /opt/elasticsearch

EXPOSE 9200 9300

# 前台启动（容器 PID 1）；堆内存由 compose 的 ES_JAVA_OPTS 控制
ENTRYPOINT ["/opt/elasticsearch/bin/elasticsearch"]
