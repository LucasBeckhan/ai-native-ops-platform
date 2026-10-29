# MCP 协议实现（模块 29 配套）

## 目录

```
agent/mcp/
├── minimal/            # ★ 手写 MCP（教学：懂协议内部）
│   ├── mcp_protocol.py # JSON-RPC 2.0 数据结构与错误码
│   ├── mcp_server.py   # Server：tools/resources/prompts 三能力，stdio+HTTP 双传输
│   └── mcp_client.py   # Client：握手/发现/调用/资源读取，双传输
├── servers/teleops-server/   # 容器化 Server（HTTP :8300，见 docker-compose.agent.yml）
├── client/                   # Harness 集成（经 harness/src/tool_router.py）
├── examples/e2e_demo.py      # ★ 端到端联调 + 直接调用 vs MCP 对比实验
├── docs/                     # 协议讲解见 ../harness/docs/01-skill-mcp-harness.md 第 4 节
└── labs/lab-29.md            # 实验指南
```

## 生产对照说明

教学实现覆盖 MCP 2024-11-05 规范的握手/发现/调用/资源/提示五类方法。
生产建议使用官方 SDK（Python: `pip install mcp`，FastMCP 服务端 + Client 会话），
差异与对照要点见 harness/docs 第 4.1/4.4 节（安全、多 Server 管理、订阅机制）。
