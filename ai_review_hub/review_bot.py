import asyncio
import os
from datetime import datetime

import discord
from discord import app_commands
from dotenv import load_dotenv

from prompts import ROUND1_TEMPLATE, ROUND2_TEMPLATE, SYNTHESIS_TEMPLATE
from providers import Providers


load_dotenv()

GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
RESULT_CHANNEL_ID = int(os.getenv("DISCORD_RESULT_CHANNEL_ID", "0"))
ARCHIVE_CHANNEL_ID = int(os.getenv("DISCORD_ARCHIVE_CHANNEL_ID", "0"))


def fmt_map(m: dict) -> str:
    return "\n\n".join([f"## {k}\n{v}" for k, v in m.items()])


class ReviewBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.providers = Providers()

    async def setup_hook(self):
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()


bot = ReviewBot()


@bot.tree.command(name="review", description="发起 AI 多模型评审")
@app_commands.describe(
    issue="议题",
    goal="目标（收益/稳定/速度）",
    constraints="硬约束",
    risk="可接受风险",
    deadline="截止时间",
)
async def review_cmd(
    interaction: discord.Interaction,
    issue: str,
    goal: str,
    constraints: str,
    risk: str,
    deadline: str = "未指定",
):
    await interaction.response.defer(thinking=True)

    # Round 1
    r1_prompt = ROUND1_TEMPLATE.format(
        issue=issue,
        goal=goal,
        constraints=constraints,
        risk=risk,
        deadline=deadline,
    )
    round1 = await bot.providers.ask_all(r1_prompt)

    # Round 2
    round2 = {}
    model_map = {
        "openai": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        "anthropic": os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022"),
        "gemini": os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
    }
    for me, ans in round1.items():
        others = {k: v for k, v in round1.items() if k != me}
        p = ROUND2_TEMPLATE.format(issue=issue, self_answer=ans, others=fmt_map(others))
        if me in model_map:
            round2[me] = await bot.providers.ask(me, model_map[me], p)
        else:
            round2[me] = ""

    # Synthesis (use openai as judge if available; else local merge)
    s_prompt = SYNTHESIS_TEMPLATE.format(
        issue=issue,
        goal=goal,
        constraints=constraints,
        risk=risk,
        round1=fmt_map(round1),
        round2=fmt_map(round2),
    )
    if os.getenv("OPENAI_API_KEY"):
        summary = await bot.providers.ask("openai", os.getenv("OPENAI_MODEL", "gpt-4o-mini"), s_prompt)
    else:
        summary = "[无 OpenAI 裁判模型，以下仅输出原始结果]\n\n" + fmt_map(round1)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    short = f"### AI评审结果（{ts}）\n**议题**: {issue}\n\n{summary[:1500]}"

    # post result
    result_channel = interaction.channel
    if RESULT_CHANNEL_ID:
        c = bot.get_channel(RESULT_CHANNEL_ID)
        if c:
            result_channel = c

    await result_channel.send(short)

    # post archive
    if ARCHIVE_CHANNEL_ID:
        c = bot.get_channel(ARCHIVE_CHANNEL_ID)
        if c:
            archive_text = (
                f"# 议题\n{issue}\n\n"
                f"## 目标\n{goal}\n\n"
                f"## 约束\n{constraints}\n\n"
                f"## 风险\n{risk}\n\n"
                f"## Round1\n{fmt_map(round1)}\n\n"
                f"## Round2\n{fmt_map(round2)}\n\n"
                f"## Final\n{summary}"
            )
            # split by discord limit
            for i in range(0, len(archive_text), 1800):
                await c.send(archive_text[i : i + 1800])

    await interaction.followup.send("评审已完成并发布到结果频道。", ephemeral=True)


if __name__ == "__main__":
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN 未配置")
    bot.run(token)
