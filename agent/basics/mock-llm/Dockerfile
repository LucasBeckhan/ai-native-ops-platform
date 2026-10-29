# =====================================================================
# 文件名: agent/basics/mock-llm/Dockerfile
# 说明: Mock LLM Server 镜像。
#   网络受限环境的离线构建模式：依赖 wheel 由 scripts/download-wheels.sh
#   在宿主机预下载到 docker/wheels/，构建时 COPY 进容器离线安装——
#   绕开"构建容器内 pip 需联网"的代理/防火墙问题。
#   网络畅通时可改回在线安装（见注释行）。
# =====================================================================
FROM python:3.11.14-slim-bookworm

WORKDIR /app

# ---- 离线安装模式（默认）：wheel 由宿主机预下载 ----
COPY docker/wheels /tmp/wheels
COPY agent/basics/mock-llm/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --no-index --find-links /tmp/wheels \
    -r /tmp/requirements.txt

# ---- 在线安装模式（网络畅通时替换上方两行）----
# COPY agent/basics/mock-llm/requirements.txt .
# RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY agent/basics/mock-llm/mock_llm_server.py .

RUN useradd -m appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "mock_llm_server:app", "--host", "0.0.0.0", "--port", "8000"]
