# Lab 27：统一适配层五个生产场景实验

> **前置**：Mock LLM 运行于 localhost:18000（`docker compose -f docker/docker-compose.llm.yml up -d` 或 uvicorn）
> **预计耗时**：60 分钟
> **实验目标**：① 跑通五个生产场景并理解每个设计决策；② 亲手注入故障验证降级与退避；③ 接入成本回调。

## 实验一：五个场景一次跑通（15 分钟）

```bash
cd agent/basics/m27-llm-adapter
python3 demo_adapter.py
```

**预期输出**（实测）：

```
============================================================
场景 1：基本调用——用量、延迟、成本三要素
   模型=mock-gpt 用量=61tok 延迟=7ms 成本=$0.000000
============================================================
场景 2：主备降级——主槽位端口不可达，备槽位自动接管
[adapter] 槽位 primary(gpt-4o-mini) 失败: gpt-4o-mini 网络错误: [Errno 61] Connection refused，尝试下一个
   最终由 mock-gpt 服务（日志可见 primary 失败→切换过程）
============================================================
场景 3：流式输出——增量回调实现打字机效果
   收到 6 个增量片段，聚合 69 字符（首个片段: '我是网络运维助手（Mock '）
============================================================
场景 4：结构化输出修复——真实模型常见'围栏包裹'的清洗
   原始输出含 markdown 围栏，清洗后: {'severity': 'critical', 'impact': '小区退服'}
============================================================
场景 5：成本核算——价目表换算与预算控制的数据基础
   gpt-4o-mini 100万入+50万出 = $0.45
```

**观察任务**：场景 2 的日志显示了降级过程；场景 3 的"6 个增量片段"就是 SSE 逐块推送的实证。

## 实验二：限流重试观测（20 分钟）

```python
# retry_probe.py —— 起一个始终返回 429 的假厂商，观察指数退避节奏
import http.server, threading, time
from llm_adapter import LLMAdapter, ProviderConfig, Provider, ChatMessage, Role

class Always429(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(429); self.end_headers()
    def log_message(self, *a): pass

srv = http.server.HTTPServer(("127.0.0.1", 19876), Always429)
threading.Thread(target=srv.serve_forever, daemon=True).start()

t0 = time.time()
try:
    LLMAdapter(providers={"only": ProviderConfig(
        Provider.OPENAI, "http://127.0.0.1:19876/v1", "X", "gpt-4o-mini"
    )}).chat([ChatMessage(Role.USER, "hi")])
except Exception as e:
    print(f"总耗时 {time.time()-t0:.1f}s（预期 ≥ 3.5s：0.5+1+2 三次退避）: {e}")
```

**预期**：`总耗时 ≈ 3.5~4s`。**思考题**：3.5s 的重试代价对"告警分析"（异步）可接受，对"用户对话"呢？（答：对话侧应缩短重试上限并更早降级——延迟预算不同，重试参数就该不同。）

## 实验三：接入成本回调（15 分钟）

```python
# cost_probe.py —— on_usage 回调聚合为一次"账单"
from llm_adapter import LLMAdapter, ChatMessage, Role, DEFAULT_PROVIDERS

ledger: list[tuple] = []
def on_usage(model, usage, cost, latency):
    ledger.append((model, usage.total, cost, latency))

a = LLMAdapter(providers={"mock": DEFAULT_PROVIDERS["mock"]}, on_usage=on_usage)
for q in ["分析告警A-1001", "分析告警A-1002", "分析告警A-1003"]:
    a.chat([ChatMessage(Role.USER, q)])
print(f"账单：{len(ledger)} 次调用，共 {sum(r[1] for r in ledger)} tokens，成本 ${sum(r[2] for r in ledger):.6f}")
```

**改造练习**：把 ledger 换成 Prometheus Counter（对照 mock_llm_server.py 的实现），再 `curl localhost:18000/metrics | grep llm_` 验证指标增长。

## 收尾思考

1. 把 primary 槽位换成真实 OpenAI 兼容服务（vLLM 本地部署/厂商 API）要改几行？（一个 ProviderConfig——适配层的价值所在）
2. extract_json 还缺什么防御？（重试一次 + 记录原始输出供排查；模块 33 护栏补全）
