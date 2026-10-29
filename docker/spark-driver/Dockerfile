# =====================================================================
# 文件名: docker/spark-driver/Dockerfile
# 所属模块: 场景二 / 模块 21-24 —— PySpark 驱动侧镜像
# 说明:
#   - Spark 集群（master/worker）用 apache/spark:v3.5.5 JVM 镜像；
#     PySpark 驱动用本镜像（python + pip 安装的 pyspark）。
#   - 为什么可以拆开：纯 DataFrame/SQL 作业的执行体在 JVM（Executor），
#     Python 只在 Driver 侧编排——无 Python UDF 的作业无需 Executor 装 Python。
#     使用 Python UDF 的作业请整体换装 PySpark 全家桶镜像（见模块 21 文档）。
#   - 依赖 wheel 由 scripts/download-spark-jars.sh 同类思路：宿主机预下载
#     （见 docker/scripts/download-wheels.sh，需追加 pyspark==3.5.5）。
# =====================================================================
FROM python:3.11.14-slim-bookworm

# pyspark 的 pip 发行版（sdist tar.gz，~300MB）自带完整 Spark 发行包，spark-submit 可用
COPY docker/wheels/pyspark-3.5.5.tar.gz docker/wheels/py4j-0.10.9.7-py2.py3-none-any.whl /tmp/
RUN pip install --no-cache-dir --no-index --find-links /tmp /tmp/pyspark-3.5.5.tar.gz \
    && rm -f /tmp/pyspark-3.5.5.tar.gz /tmp/py4j-0.10.9.7-py2.py3-none-any.whl

# 提交作业的入口脚本：指向 Standalone Master
COPY docker/spark-driver/submit.sh /usr/local/bin/submit
RUN chmod +x /usr/local/bin/submit && useradd -m appuser
USER appuser
WORKDIR /opt/spark/work-dir

# 默认显示帮助；实际提交：submit jobs/offline_attack_analysis.py [extra args]
CMD ["submit", "--help"]
