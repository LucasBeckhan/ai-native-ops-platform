#!/usr/bin/env bash
# =====================================================================
# 文件名: docker/scripts/download-wheels.sh
# 所属模块: 离线构建的前置步骤（配合各 Dockerfile 的离线安装模式）
# 运行方式: ./scripts/download-wheels.sh    （在 docker/ 目录执行）
# 说明:
#   - 在宿主机（网络可用、可配代理）预下载 linux/aarch64 + pure-python
#     的全部依赖 wheel 到 wheels/，供容器内 --no-index 离线安装。
#   - 适用本项目全部 Python 服务（mock-llm / agent-api / 后续 RAG 服务）。
#   - Apple Silicon 宿主机下载（--platform manylinux2014_aarch64），
#     x86 机器请改 manylinux2014_x86_64。
# =====================================================================
set -euo pipefail

mkdir -p wheels
pip3 download -q -d wheels \
  --platform manylinux2014_aarch64 --platform any \
  --only-binary=:all: --python-version 311 \
  -i https://pypi.tuna.tsinghua.edu.cn/simple \
  fastapi==0.111.0 uvicorn==0.30.1 httpx==0.27.0 \
  prometheus-client==0.20.0 pydantic==2.7.4

echo "已下载 $(ls wheels | wc -l | tr -d ' ') 个 wheel 到 docker/wheels/"
