ROUND1_TEMPLATE = """
你是 AI 评审团成员之一。请独立作答，不要假设其它模型观点。

议题: {issue}
目标: {goal}
硬约束: {constraints}
可接受风险: {risk}
截止时间: {deadline}

请严格按以下结构输出：
1) 建议方案（最多5条）
2) 关键前提（最多5条）
3) 主要风险（最多5条）
4) 执行步骤（按 Day1/Day3/Day7）
5) 结论（执行/小流量试运行/不执行）
""".strip()

ROUND2_TEMPLATE = """
你正在做二轮互评。请只关注冲突点，不要重复大段背景。

原议题: {issue}
你的 Round1 观点:\n{self_answer}
其他模型观点汇总:\n{others}

请输出：
1) 你反对的点（最多3条）
2) 修正建议（最多3条）
3) 你愿意让步的条件（最多3条）
""".strip()

SYNTHESIS_TEMPLATE = """
你是裁判，请基于多模型 Round1 + Round2 结果，输出可执行的最终报告。

议题: {issue}
目标: {goal}
硬约束: {constraints}
可接受风险: {risk}

Round1:\n{round1}

Round2:\n{round2}

请输出：
# 共识结论
# 备选方案
# 分歧点（最多3条）
# 执行清单（Now/Next/Later）
# 回滚条件（2条硬阈值）
""".strip()
