# =====================================================================
# 文件名: platform/agent-service/Dockerfile
# 说明: Agent API 服务镜像。依赖（fastapi/httpx/prometheus-client/pydantic）
#       与 Mock LLM 一致；业务代码（agent/ 目录）由 compose 挂载注入，
#       教学场景改代码无需重建镜像。
# =====================================================================
FROM python:3.11.14-slim-bookworm

WORKDIR /app

# 离线安装：wheel 由 docker/scripts/download-wheels.sh 预下载（构建上下文=docker/）
COPY docker/wheels /tmp/wheels
RUN pip install --no-cache-dir --no-index --find-links /tmp/wheels \
    fastapi==0.111.0 uvicorn==0.30.1 httpx==0.27.0 prometheus-client==0.20.0 pydantic==2.7.4

# 业务代码由 compose 挂载到 /app/agent（PYTHONPATH 由 compose 注入）
COPY platform/agent-service/agent_api.py /app/agent_api.py

RUN useradd -m appuser
USER appuser

EXPOSE 8100

CMD ["uvicorn", "agent_api:app", "--host", "0.0.0.0", "--port", "8100"]
