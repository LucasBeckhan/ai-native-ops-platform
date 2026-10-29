#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m26-prompts/sampling_demo.py
# 所属模块: 模块 26 —— Temperature/TopP 采样机制可视化
# 运行方式: python3 sampling_demo.py
# 说明:
#   - Mock LLM 是确定性实现（保证实验可断言），无法展示采样随机性；
#     因此本脚本在本地构造"下一 Token 概率分布"，亲手实现
#     Temperature 缩放与 TopP 截断两种采样策略，让机制可观察。
#   - 与真实模型的关系：真实 LLM 输出的就是这样的分布，logits 经
#     softmax 后按策略采样——你看完本脚本就完全理解了这两个参数。
# =====================================================================
import random

# 模拟"告警分级"任务下一步的候选 Token 分布（logits softmax 后的概率）
CANDIDATES = {
    "critical": 0.42,
    "major": 0.31,
    "minor": 0.15,
    "unknown": 0.07,
    "needs_info": 0.05,
}


def apply_temperature(dist: dict[str, float], t: float) -> dict[str, float]:
    """Temperature 缩放的原理是对 logits 除以 T 再 softmax。
    直接对概率操作时等价实现：p_i^(1/T) 后归一化——T 越小分布越尖。"""
    if t <= 0:
        # T=0 的极限：贪心解码，全部概率给 argmax
        best = max(dist, key=dist.get)
        return {k: (1.0 if k == best else 0.0) for k in dist}
    scaled = {k: v ** (1.0 / t) for k, v in dist.items()}
    z = sum(scaled.values())
    return {k: v / z for k, v in scaled.items()}


def apply_topp(dist: dict[str, float], p: float) -> dict[str, float]:
    """TopP（核采样）：按概率降序累积，只保留累积到 P 的候选，重新归一化。"""
    items = sorted(dist.items(), key=lambda kv: -kv[1])
    kept, acc = {}, 0.0
    for k, v in items:
        kept[k] = v
        acc += v
        if acc >= p:
            break
    z = sum(kept.values())
    return {k: v / z for k, v in kept.items()}


def sample(dist: dict[str, float], n: int = 8, seed: int = 42) -> list[str]:
    """按分布采样 n 次（固定随机种子保证实验可复现）。"""
    rng = random.Random(seed)
    ks, vs = zip(*dist.items())
    return rng.choices(ks, weights=vs, k=n)


def main() -> None:
    print("原始分布:", CANDIDATES)

    for t in (0.0, 0.5, 1.5):
        d = apply_temperature(CANDIDATES, t)
        print(f"\n== Temperature={t}（{'贪心' if t == 0 else ('低温聚焦' if t < 1 else '高温发散')}） ==")
        print("  分布:", {k: round(v, 3) for k, v in d.items()})
        print("  采样 8 次:", sample(d))

    print("\n== TopP 截断对比 ==")
    for p in (0.5, 0.9, 1.0):
        d = apply_topp(CANDIDATES, p)
        print(f"  TopP={p}: 保留 {list(d)} | 分布: { {k: round(v, 3) for k, v in d.items()} }")

    print("\n== 工程结论 ==")
    print("  1) 告警分级/字段抽取用 T=0（贪心）→ 输出稳定可解析")
    print("  2) 根因推理 T=0.2~0.5，兼顾发散与可控")
    print("  3) TopP 与 Temperature 二选一调节即可，同时调大容易失控")
    print("  4) 真实模型上这两个参数作用于同样的机制——Mock 模式下行为恒定是为了测试可断言")


if __name__ == "__main__":
    main()
